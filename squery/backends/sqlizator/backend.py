"""
Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from __future__ import print_function

import calendar
import contextlib
import datetime
import logging

import pyqlizator

from sqlize import (From, Where, Group, Order, Limit, Select, Update, Delete,
                    Insert, Replace, sqlin, sqlarray)
from pytz import utc

from .pool import ConnectionPool


SQLITE_DATE_TYPES = ('date', 'datetime', 'timestamp')


def from_utc_timestamp(timestamp):
    """Converts the passed-in unix UTC timestamp into a datetime object."""
    dt = datetime.datetime.utcfromtimestamp(float(timestamp))
    return dt.replace(tzinfo=utc)


def to_utc_timestamp(dt):
    """Converts the passed-in datetime object into a unix UTC timestamp."""
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        msg = "Naive datetime object passed. It is assumed that it's in UTC."
        logging.warning(msg)
    return calendar.timegm(dt.timetuple())


pyqlizator.to_primitive_converter(datetime.datetime, to_utc_timestamp)
for date_type in SQLITE_DATE_TYPES:
    pyqlizator.from_primitive_converter(date_type, from_utc_timestamp)


class Backend(object):
    MAX_VARIABLE_NUMBER = pyqlizator.MAX_VARIABLE_NUMBER
    # Provide access to query classes for easier access
    sqlin = staticmethod(sqlin)
    sqlarray = staticmethod(sqlarray)
    From = From
    Where = Where
    Group = Group
    Order = Order
    Limit = Limit
    Select = Select
    Update = Update
    Delete = Delete
    Insert = Insert
    Replace = Replace

    ConnectionPool = ConnectionPool

    def __init__(self, **kwargs):
        self._conn_params = kwargs
        self._pool = self.create_pool(**self._conn_params)

    def execute(self, sql, *parameters):
        with self._pool.connection() as conn:
            return conn.cursor().execute(sql, *parameters)

    def executemany(self, sql, seq_of_params):
        with self._pool.connection() as conn:
            return conn.cursor().executemany(sql, seq_of_params)

    def executescript(self, sql):
        with self._pool.connection() as conn:
            return conn.cursor().executescript(sql)

    def fetchone(self, sql, *parameters):
        with self._pool.connection() as conn:
            return conn.cursor().fetchone(sql, *parameters)

    def fetchall(self, sql, *parameters):
        with self._pool.connection() as conn:
            return conn.cursor().fetchall(sql, *parameters)

    def fetchiter(self, sql, *parameters):
        with self._pool.connection() as conn:
            return conn.cursor().fetchiter(sql, *parameters)

    @contextlib.contextmanager
    def transaction(self):
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute('BEGIN;')
            try:
                yield cursor
            except Exception:
                cursor.execute('ROLLBACK;')
            else:
                cursor.execute('COMMIT;')

    def close(self):
        self._pool.closeall()

    @classmethod
    def create_pool(cls, host, port, database, path):
        return cls.ConnectionPool(pyqlizator.Connection,
                                  host=host,
                                  port=port,
                                  database=database,
                                  path=path)

    def recreate(self):
        with self._pool.connection() as conn:
            conn.drop_database()
        self.close()
        self._pool = self.create_pool(**self._conn_params)
