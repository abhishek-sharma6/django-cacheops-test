import django.dispatch

cache_read = django.dispatch.Signal()
cache_invalidated = django.dispatch.Signal()
