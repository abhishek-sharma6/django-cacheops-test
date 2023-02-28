import django.dispatch

cache_read = django.dispatch.Signal("func", "hit")
cache_invalidated = django.dispatch.Signal("obj_dict")
