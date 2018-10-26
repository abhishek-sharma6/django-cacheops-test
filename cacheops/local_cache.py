
class CacheLocal(object):
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
