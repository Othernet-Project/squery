"""
squery.py: A proxy providing a unified interface to different database backends

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from __future__ import print_function

import functools
import importlib
import re

from .migrations import migrate
from .utils import basestring


SLASH = re.compile(r'\\')


class Database(object):
    migrate = staticmethod(migrate)
    _error_handlers = []

    def __init__(self, backend, debug=False):
        self._backend = backend
        self._debug = debug

    def __getattr__(self, name):
        return getattr(self._backend, name)

    def serialize_query(func):
        """ Ensure any SQLExpression instances are serialized"""
        @functools.wraps(func)
        def wrapper(self, query, *args, **kwargs):
            if hasattr(query, 'serialize'):
                query = query.serialize()

            assert isinstance(query, basestring), 'Expected query to be string'
            if self._debug:
                print('SQL:', query)

            return func(self, query, *args, **kwargs)
        return wrapper

    def guard(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as exc:
                for fn in self._error_handlers:
                    fn(self, exc)
                raise
        return wrapper

    @guard
    @serialize_query
    def execute(self, query, *params):
        return self._backend.execute(query, *params)

    @guard
    @serialize_query
    def executemany(self, sql, seq_of_params):
        return self._backend.executemany(sql, seq_of_params)

    @guard
    def executescript(self, sql):
        return self._backend.executescript(sql)

    @guard
    @serialize_query
    def fetchone(self, sql, *params):
        return self._backend.fetchone(sql, *params)

    @guard
    @serialize_query
    def fetchall(self, sql, *params):
        return self._backend.fetchall(sql, *params)

    @guard
    @serialize_query
    def fetchiter(self, sql, *params):
        return self._backend.fetchiter(sql, *params)

    @guard
    def transaction(self, *args, **kwargs):
        return self._backend.transaction(*args, **kwargs)

    @guard
    def close(self):
        self._backend.close()

    @classmethod
    def get_backend_class(cls, name):
        mod_path = '.'.join(['squery', 'backends', name, 'backend'])
        mod = importlib.import_module(mod_path)
        return getattr(mod, 'Backend')

    @classmethod
    def connect(cls, backend, debug=False, *args, **kwargs):
        backend_cls = cls.get_backend_class(backend)
        return cls(backend_cls(*args, **kwargs), debug=debug)

    @guard
    def recreate(self):
        self._backend.recreate()

    @classmethod
    def on_error(cls, callback):
        cls._on_error.append(callback)


class DatabaseContainer(dict):

    def __init__(self, databases, **kwargs):
        super(DatabaseContainer, self).__init__(databases)
        self.__dict__ = self

