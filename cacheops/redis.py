from __future__ import absolute_import
import warnings
from contextlib import contextmanager
import six

from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string

from funcy import decorator, identity, memoize, LazyObject
import redis
from redis.sentinel import Sentinel
from .conf import settings
from rediscluster import StrictRedisCluster

if settings.CACHEOPS_DEGRADE_ON_FAILURE:
    @decorator
    def handle_connection_failure(call):
        try:
            return call()
        except redis.ConnectionError as e:
            warnings.warn("The cacheops cache is unreachable! Error: %s" % e, RuntimeWarning)
        except redis.TimeoutError as e:
            warnings.warn("The cacheops cache timed out! Error: %s" % e, RuntimeWarning)
else:
    handle_connection_failure = identity

LOCK_TIMEOUT = 60


class SafeRedisCluster(StrictRedisCluster):
    get = handle_connection_failure(StrictRedisCluster.get)
    _local_hash_key_cache = {}
    _cluster_state = None

    def __init__(self, *args, **kwargs):
        super(SafeRedisCluster, self).__init__(*args, **kwargs)
        self.build_local_hash_key_cache()

    def get_hashed_key(self, cache_key):
        if id(self.connection_pool.nodes.nodes) != self._cluster_state:
            self.build_local_hash_key_cache()
        node_name = self.connection_pool.nodes.node_from_slot(self.connection_pool.nodes.keyslot(hash(cache_key)))[
            "name"]
        return self._local_hash_key_cache[node_name]

    def execute_command(self, *args, **options):
        try:
            return super(SafeRedisCluster, self).execute_command(*args, **options)
        except redis.ResponseError as e:
            if "READONLY" not in e.message:
                raise
            connection = self.connection_pool.get_connection(args[0], **options)
            connection.disconnect()
            warnings.warn("Primary probably failed over, reconnecting")
            return super(SafeRedisCluster, self).execute_command(*args, **options)

    def build_local_hash_key_cache(self):
        hash_key = 0
        master_nodes = list(
            node for node in self.connection_pool.nodes.nodes.values() if node["server_type"] == "master")
        print("connectinpool nodes:  ,", self.connection_pool.nodes)
        print("connectinpool nodes nodes:  ,", self.connection_pool.nodes.nodes)
        print("connectinpool nodes nodes values:  ,", self.connection_pool.nodes.nodes.values)
        print("master nodes: ", master_nodes)
        while len(self._local_hash_key_cache) < len(master_nodes):
            node = self.connection_pool.nodes.node_from_slot(
                self.connection_pool.nodes.keyslot(str(hash_key)))
            print("node: " , node)
            self._local_hash_key_cache[node["name"]] = "{%s}" % str(hash_key)
            hash_key += 1

        self._cluster_state = id(self.connection_pool.nodes.nodes)


class SafeRedisNormal(redis.StrictRedis):
    get = handle_connection_failure(redis.StrictRedis.get)
    """ Handles failover of AWS elasticache
    """

    def execute_command(self, *args, **options):
        try:
            return super(SafeRedisNormal, self).execute_command(*args, **options)
        except redis.ResponseError as e:
            if "READONLY" not in e.message:
                raise
            connection = self.connection_pool.get_connection(args[0], **options)
            connection.disconnect()
            warnings.warn("Primary probably failed over, reconnecting")
            return super(SafeRedisNormal, self).execute_command(*args, **options)


redis_conf = settings.CACHEOPS_REDIS
if redis_conf and 'startup_nodes' in redis_conf:
    SafeRedis = SafeRedisCluster
else:
    SafeRedis = SafeRedisNormal


class CacheopsRedis(SafeRedis):
    super_get = handle_connection_failure(SafeRedis.get)

    def get_from_main(self, *args, **kwargs):
        return self.super_get(*args, **kwargs)

    @contextmanager
    def getting(self, key, lock=False):
        if not lock:
            yield self.get(key)
        else:
            locked = False
            try:
                data = self._get_or_lock(key)
                locked = data is None
                yield data
            finally:
                if locked:
                    self._release_lock(key)

    @handle_connection_failure
    def _get_or_lock(self, key):
        self._lock = getattr(self, '_lock', self.register_script("""
            local locked = redis.call('set', KEYS[1], 'LOCK', 'nx', 'ex', ARGV[1])
            if locked then
                redis.call('del', KEYS[2])
            end
            return locked
        """))
        signal_key = key + ':signal'
        while True:
            data = self.get(key)
            if data is None:
                if self._lock(keys=[key, signal_key], args=[LOCK_TIMEOUT]):
                    return None
            elif data != b'LOCK':
                return data

            # No data   and not locked, wait
            self.brpoplpush(signal_key, signal_key, timeout=LOCK_TIMEOUT)

    @handle_connection_failure
    def _release_lock(self, key):
        self._unlock = getattr(self, '_unlock', self.register_script("""
            if redis.call('get', KEYS[1]) == 'LOCK' then
                redis.call('del', KEYS[1])
            end
            redis.call('lpush', KEYS[2], 1)
            redis.call('expire', KEYS[2], 1)
        """))
        signal_key = key + ':signal'
        self._unlock(keys=[key, signal_key])


@LazyObject
def redis_client():
    if settings.CACHEOPS_REDIS and settings.CACHEOPS_SENTINEL:
        raise ImproperlyConfigured("CACHEOPS_REDIS and CACHEOPS_SENTINEL are mutually exclusive")

    client_class = CacheopsRedis
    if settings.CACHEOPS_CLIENT_CLASS:
        client_class = import_string(settings.CACHEOPS_CLIENT_CLASS)

    if settings.CACHEOPS_SENTINEL:
        if not {'locations', 'service_name'} <= set(settings.CACHEOPS_SENTINEL):
            raise ImproperlyConfigured("Specify locations and service_name for CACHEOPS_SENTINEL")

        sentinel = Sentinel(settings.CACHEOPS_SENTINEL['locations'])
        return sentinel.master_for(
            settings.CACHEOPS_SENTINEL['service_name'],
            redis_class=client_class,
            db=settings.CACHEOPS_SENTINEL.get('db', 0),
            socket_timeout=settings.CACHEOPS_SENTINEL.get('socket_timeout')
        )

    # Allow client connection settings to be specified by a URL.
    if isinstance(settings.CACHEOPS_REDIS, six.string_types):
        return client_class.from_url(settings.CACHEOPS_REDIS)
    else:
        return client_class(**settings.CACHEOPS_REDIS)


use_hash_keys = redis_conf and 'startup_nodes' in redis_conf
use_gevent = settings.CACHEOPS_USE_GEVENT
script_timeout = settings.CACHEOPS_SCRIPT_TIMEOUT
max_invalidation = settings.CACHEOPS_MAX_INVALIDATION

### Lua script loader

import re
import os.path

STRIP_RE = re.compile(r'TOSTRIP.*/TOSTRIP', re.S)


@memoize
def load_script(name, strip=False):
    filename = os.path.join(os.path.dirname(__file__), 'lua/%s.lua' % name)
    with open(filename) as f:
        code = f.read()
    if strip:
        code = STRIP_RE.sub('', code)
    return redis_client.register_script(code)
