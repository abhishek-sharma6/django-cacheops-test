from django.core.signals import request_started
import re

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
        self.local_cache[key] = val

    def get(self, key):
        if key in self.local_cache:
            return self.local_cache[key]
        return None

    def delete_key(self, key):
        return self.local_cache.pop(key, None)


CacheLocalObj = CacheLocal()


class RequestLocalCache(object):
    """
        This is used to cache queries locally for a request
    """
    request_local = None
    key = 'cached_query'
    METHOD = "POST"
    GET = "GET"  # THIS WE ARE USING TO CACHE ONLY FOR GET METHODS
    Regex_exclude_models = settings.CACHEOPS_LOCAL_CACHE_EXCLUDE_MODELS

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
        cached_query = getattr(self.get_local(), self.key, None)
        if not cached_query:
            cached_query = {}
        cached_query[key] = val
        setattr(self.get_local(), self.key, cached_query)

    def get(self, key):
        # we want to cache only for GET methods
        if self.METHOD != self.GET:
            return None
        cached_query = getattr(self.get_local(), self.key, None)
        if not cached_query or key not in cached_query:
            return None
        return cached_query[key]

    def invalidate(self):
        setattr(self.request_local, self.key, {})

    def cache_model(self, db_table_name):
        if self.combined_regex and re.match(self.combined_regex, db_table_name):
            return False
        return True


RequestLocalCacheObj = RequestLocalCache()


def on_request_start(sender, environ, **kwargs):
    RequestLocalCacheObj.clear()
    RequestLocalCacheObj.METHOD = environ["REQUEST_METHOD"]


request_started.connect(on_request_start)
