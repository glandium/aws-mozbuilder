# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


class SingletonMeta(type):
    def __init__(cls, name, bases, dct):
        type.__init__(cls, name, bases, dct)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = type.__call__(cls, *args, **kwargs)
        return cls._instance


class Singleton(object):
    __metaclass__ = SingletonMeta


class cached_property(object):
    def __init__(self, func):
        self._func = func
        self._name = '_%s' % func.__name__

    def __get__(self, obj, cls=None):
        if not hasattr(obj, self._name):
            setattr(obj, self._name, self._func(obj))
        return getattr(obj, self._name)
