# -*- coding: utf-8 -*-
import json
import threading
from funcy import memoize, post_processing, ContextDecorator
from django.db import DEFAULT_DB_ALIAS
from django.db.models.expressions import F, Expression
from distutils.version import StrictVersion

from .conf import settings
from .utils import NOT_SERIALIZED_FIELDS
from .sharding import get_prefix
from .redis import redis_client, handle_connection_failure, load_script, use_hash_keys, use_gevent, max_invalidation, \
    script_timeout
from .signals import cache_invalidated
from .transaction import queue_when_in_transaction
from .local_cache import RequestLocalCacheObj
import logging
from datetime import datetime
import pytz

__all__ = ('invalidate_obj', 'invalidate_model', 'invalidate_all', 'no_invalidation')


@memoize
def redis_can_unlink():
    # TODO please fix
    redis_version = '4.0'  # redis_client.info()['redis_version']
    return StrictVersion(redis_version) >= StrictVersion('4.0')


@queue_when_in_transaction
@handle_connection_failure
def invalidate_dict(model, obj_dict, using=DEFAULT_DB_ALIAS):
    try:
        if no_invalidation.active or not settings.CACHEOPS_ENABLED:
            return
        RequestLocalCacheObj.invalidate()
        model = model._meta.concrete_model
        prefix = get_prefix(_cond_dnfs=[(model._meta.db_table, list(obj_dict.items()))], dbs=[using])
        if use_hash_keys:
            if use_gevent:
                import gevent
                jobs = [gevent.spawn(
                    lambda key: load_script('invalidate', strip=redis_can_unlink())(
                        keys=[redis_client._local_hash_key_cache[key]], args=[
                            model._meta.db_table,
                            json.dumps(obj_dict, default=str),
                            script_timeout, max_invalidation
                        ]), key) for key in redis_client._local_hash_key_cache]
                gevent.wait(jobs)
            else:
                for key in redis_client._local_hash_key_cache:
                    load_script('invalidate', strip=redis_can_unlink())(keys=[redis_client._local_hash_key_cache[key]],
                                                                        args=[
                                                                            model._meta.db_table,
                                                                            json.dumps(obj_dict, default=str),
                                                                            script_timeout, max_invalidation
                                                                        ])
        else:
            load_script('invalidate', strip=redis_can_unlink())(keys=[prefix], args=[
                model._meta.db_table,
                json.dumps(obj_dict, default=str), script_timeout, max_invalidation
            ])
        cache_invalidated.send(sender=model, obj_dict=obj_dict)
    except Exception as e:
        if settings.CACHEOPS_LOGGING:
            cacheops_logger = logging.getLogger('cacheops.log')
            cacheops_logger.info({"time": datetime.utcnow().replace(tzinfo=pytz.UTC), "error": e})
        raise e


def invalidate_obj(obj, using=DEFAULT_DB_ALIAS):
    """
    Invalidates caches that can possibly be influenced by object
    """
    model = obj.__class__._meta.concrete_model
    invalidate_dict(model, get_obj_dict(model, obj), using=using)


@queue_when_in_transaction
@handle_connection_failure
def invalidate_model(model, using=DEFAULT_DB_ALIAS):
    """
    Invalidates all caches for given model.
    NOTE: This is a heavy artillery which uses redis KEYS request,
          which could be relatively slow on large datasets.
    """
    if no_invalidation.active or not settings.CACHEOPS_ENABLED:
        return
    model = model._meta.concrete_model
    # NOTE: if we use sharding dependent on DNF then this will fail,
    #       which is ok, since it's hard/impossible to predict all the shards
    prefix = get_prefix(tables=[model._meta.db_table], dbs=[using])
    conjs_keys = redis_client.keys('%sconj:%s:*' % (prefix, model._meta.db_table))
    if conjs_keys:
        cache_keys = redis_client.sunion(conjs_keys)
        keys = list(cache_keys) + conjs_keys
        if redis_can_unlink():
            redis_client.execute_command('UNLINK', *keys)
        else:
            redis_client.delete(*keys)
    cache_invalidated.send(sender=model, obj_dict=None)


@handle_connection_failure
def invalidate_all():
    if no_invalidation.active or not settings.CACHEOPS_ENABLED:
        return
    redis_client.flushdb()
    cache_invalidated.send(sender=None, obj_dict=None)


class InvalidationState(threading.local):
    def __init__(self):
        self.depth = 0


class _no_invalidation(ContextDecorator):
    state = InvalidationState()

    def __enter__(self):
        self.state.depth += 1

    def __exit__(self, type, value, traceback):
        self.state.depth -= 1

    @property
    def active(self):
        return self.state.depth


no_invalidation = _no_invalidation()


### ORM instance serialization

@memoize
def serializable_fields(model):
    return tuple(f for f in model._meta.fields
                 if not isinstance(f, NOT_SERIALIZED_FIELDS))


@post_processing(dict)
def get_obj_dict(model, obj):
    for field in serializable_fields(model):
        value = getattr(obj, field.attname)
        if value is None:
            yield field.attname, None
        elif isinstance(value, (F, Expression)):
            continue
        else:
            yield field.attname, field.get_prep_value(value)
