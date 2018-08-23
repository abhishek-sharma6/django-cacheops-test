from __future__ import absolute_import

import random
import sys
import traceback
import warnings
from contextlib import contextmanager

import redis
import six
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string
from funcy import decorator, identity, memoize, LazyObject
from redis.sentinel import Sentinel

from .conf import settings

if settings.CACHEOPS_DEGRADE_ON_FAILURE:
    @decorator
    def handle_connection_failure(call):
        try:
            return call()
        except redis.ConnectionError as e:
            warnings.warn("The cacheops cache is unreachable! Error: %s" % e, RuntimeWarning)
        except redis.TimeoutError as e:
            warnings.warn("The cacheops cache timed out! Error: %s" % e, RuntimeWarning)
        except Exception as e:
            warnings.warn("".join(traceback.format_exception(*sys.exc_info())))
else:
    handle_connection_failure = identity

LOCK_TIMEOUT = 60


class SafeRedis(redis.StrictRedis):
    get = handle_connection_failure(redis.StrictRedis.get)
    """ Handles failover of AWS elasticache
    """

    def execute_command(self, *args, **options):
        try:
            return super(SafeRedis, self).execute_command(*args, **options)
        except redis.ResponseError as e:
            if "READONLY" not in e.message:
                raise
            connection = self.connection_pool.get_connection(args[0], **options)
            connection.disconnect()
            warnings.warn("Primary probably failed over, reconnecting")
            return super(SafeRedis, self).execute_command(*args, **options)


# CacheopsRedis = SafeRedis

redis_replicas = None
try:
    # the conf could be a list of string
    # list would look like: ["redis://cache-001:6379/1", "redis://cache-002:6379/2"]
    # string would be: "redis://cache-001:6379/1,redis://cache-002:6379/2"
    redis_replica_conf = settings.CACHEOPS_REDIS_REPLICA
    if redis_replica_conf:
        if isinstance(redis_replica_conf, six.string_types):
            redis_replicas = map(redis.StrictRedis.from_url, redis_replica_conf.split(','))
        else:
            redis_replicas = []
            for replica in redis_replica_conf:
                redis_replicas.append(redis.StrictRedis(**replica))
except AttributeError as err:
    pass


class CacheopsRedis(SafeRedis):
    super_get = handle_connection_failure(SafeRedis.get)

    def get(self, *args, **kwargs):
        if redis_replicas:
            try:
                redis_replica = random.choice(redis_replicas)
                return redis_replica.get(*args, **kwargs)
            except redis.ConnectionError:
                pass
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

            # No data and not locked, wait
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
