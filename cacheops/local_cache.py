from django.core.signals import request_started
import re
from .cross import pickle

from cacheops.conf import settings


class CacheLocal(object):
    """
        The caching is for process level
    """
    local_cache = {}
    CachedData = 'cached_data'
    Expiry = 'expiry'

    '''
        local_cache will have all the cached data in the format
        1. local_cache = {
                        'some_key' : {
                                        'cached_data' : data,
                                        'expiry' : time in epoch when it will be expired
                                     },
                        'some_key' : {
                                        'cached_data' : data,
                                        'expiry' : time in epoch when it will be expired
                                     },
                      }
        2. If expiry is set to -1 then timeout is not specified
    '''

    def clear(self):
        self.local_cache = {}

    def set(self, key, val):
        self.local_cache[key] = pickle.dumps(val, -1)

    def get(self, key):
        data = self.local_cache.get(key)
        return pickle.loads(data) if data else None

    def delete_key(self, key):
        return self.local_cache.pop(key, None)


CacheLocalObj = CacheLocal()


class RequestLocalCache(object):
    """
        This is used to cache queries locally for a request
    """
    request_local = None
    key = 'cached_query'
    GET = "GET"  # THIS WE ARE USING TO CACHE ONLY FOR GET METHODS
    POST = "POST"
    Regex_exclude_models = settings.CACHEOPS_LOCAL_CACHE_EXCLUDE_MODELS
    METHOD = "METHOD"  # the method for every request is stored in request_local with the name METHOD

    def __init__(self):
        if self.Regex_exclude_models:
            self.combined_regex = "(" + ")|(".join(self.Regex_exclude_models) + ")" # instead of doing everytime it is sotred here
        else:
            self.combined_regex = ""

    def get_local(self):
        if not self.request_local or not hasattr(self.request_local, self.key):
            from _threading_local import local

            class unlocal(local):
                def __del__(self):
                    try:
                        super(unlocal).__del__(self)
                    except:
                        pass

            self.request_local = unlocal()
            setattr(self.request_local, self.key, {})
        return self.request_local

    def clear(self):
        setattr(self.get_local(), self.key, {})

    def set(self, key, val):
        if not self.is_get_request():
            return
        cached_query = getattr(self.get_local(), self.key, None)
        if not cached_query:
            cached_query = {}
        cached_query[key] = val
        setattr(self.get_local(), self.key, cached_query)

    def get(self, key):
        if not self.is_get_request():
            return None
        # we want to cache only for GET methods
        if not self.is_get_request():
            return None
        cached_query = getattr(self.get_local(), self.key, None)
        if not cached_query or key not in cached_query:
            return None
        return cached_query[key]

    def invalidate(self):
        if not self.is_get_request():
            return
        setattr(self.get_local(), self.key, {})

    def cache_model(self, db_table_name):
        if not self.is_get_request():
            return False
        if self.combined_regex and re.match(self.combined_regex, db_table_name):
            return False
        return True

    def set_request_related_varibles(self, method=POST):
        setattr(self.get_local(), self.METHOD, method)

    def is_get_request(self):
        method = getattr(self.get_local(), self.METHOD, self.POST)
        return method == self.GET


RequestLocalCacheObj = RequestLocalCache()


def on_request_start(sender, environ, **kwargs):
    pass
    # RequestLocalCacheObj.clear()
    # method = environ.get("REQUEST_METHOD", None)
    # RequestLocalCacheObj.set_request_related_varibles(method=method)


request_started.connect(on_request_start)
