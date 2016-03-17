"""
Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from __future__ import print_function

import calendar
import datetime
import logging

from sqlize import (From, Where, Group, Order, Limit, Select, Update, Delete,
                    Insert, Replace, sqlin, sqlarray)
from pytz import utc

from .pool import Connection, ConnectionPool


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

    def execute(self, sql, *params):
        return self._pool.execute(sql, *params)

    def executemany(self, sql, seq_of_params):
        return [self._pool.execute(sql, params) for params in seq_of_params]

    def executescript(self, sql):
        return self._pool.execute(sql)

    def fetchone(self, sql, *parameters):
        for item in self._pool.fetch(sql, *parameters):
            return item  # returns first item received and ignores rest

    def fetchall(self, sql, *parameters):
        return list(self._pool.fetch(sql, *parameters))

    def fetchiter(self, sql, *parameters):
        return self._pool.fetch(sql, *parameters)

    def transaction(self, *args, **kwargs):
        return self._pool.connection()

    def close(self):
        self._pool.closeall()

    @classmethod
    def create_pool(cls, host, port, database, path):
        return cls.ConnectionPool(host=host,
                                  port=port,
                                  database=database,
                                  path=path)

    @classmethod
    def command(cls, host, port, database, path):
        pool = cls.create_pool(host, port, database, path)

    @classmethod
    def create(cls, host, port, database, path):
        cls.command(host, port, database, path)

    @classmethod
    def drop(cls, host, port, database, path):
        cls.command(host, port, database, path)

    def recreate(self):
        self.close()
        self.drop(**self._conn_params)
        self.create(**self._conn_params)

