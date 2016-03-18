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

from sqlize import (From, Where, Group, Order, Limit, Select, Update, Delete,
                    Insert, Replace, sqlin, sqlarray)
from pytz import utc
from pyqlizator import Connection

from .pool import ConnectionPool


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


Connection.register_to_primitive(datetime.datetime, to_utc_timestamp)
for date_type in Connection.SQLITE_DATE_TYPES:
    Connection.register_from_primitive(date_type, from_utc_timestamp)


class Backend(object):
    MAX_VARIABLE_NUMBER = Connection.MAX_VARIABLE_NUMBER
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
            return conn.execute(sql, *parameters)

    def executemany(self, sql, seq_of_params):
        with self._pool.connection() as conn:
            return conn.executemany(sql, seq_of_params)

    def executescript(self, sql):
        with self._pool.connection() as conn:
            return conn.executescript(sql)

    def fetchone(self, sql, *parameters):
        with self._pool.connection() as conn:
            return conn.fetchone(sql, *parameters)

    def fetchall(self, sql, *parameters):
        with self._pool.connection() as conn:
            return conn.fetchall(sql, *parameters)

    def fetchiter(self, sql, *parameters):
        with self._pool.connection() as conn:
            return conn.fetchiter(sql, *parameters)

    @contextlib.contextmanager
    def transaction(self):
        with self._pool.connection() as conn:
            conn.execute('BEGIN;')
            try:
                yield conn
            except Exception:
                conn.execute('ROLLBACK;')
            else:
                conn.execute('COMMIT;')

    def close(self):
        self._pool.closeall()

    @classmethod
    def create_pool(cls, host, port, database, path):
        return cls.ConnectionPool(Connection,
                                  host=host,
                                  port=port,
                                  database=database,
                                  path=path)

    def recreate(self):
        with self._pool.connection() as conn:
            conn.drop_database()
        self.close()
        self._pool = self.create_pool(**self._conn_params)
