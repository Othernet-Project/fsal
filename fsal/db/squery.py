"""
sqery.py: Helpers for working with databases

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from __future__ import print_function

import inspect
import functools
import re

import psycopg2

from psycopg2.extras import NamedTupleCursor

from ..utils import basestring
from sqlize import (From, Where, Group, Order, Limit, Select, Update, Delete,
                    Insert, Replace, sqlin, sqlarray)

from .pool import PostgresConnectionPool


AUTOCOMMIT = psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
SLASH = re.compile(r'\\')
MAX_VARIABLE_NUMBER = 999
DEFAULT_MAX_POOL_SIZE = 5


psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


def call_relevant_args(f, kwargs):
    valid_args = inspect.getargspec(f).args
    relevant_args = {k: v for k,v in kwargs.items() if k in valid_args}
    f(**relevant_args)


class Database(object):

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
    MAX_VARIABLE_NUMBER = MAX_VARIABLE_NUMBER

    def __init__(self, pool, connection_params, debug=False):
        self.pool = pool
        self.connection_params = connection_params
        self.debug = debug

    def serialize_query(func):
        """ Ensure any SQLExpression instances are serialized"""
        @functools.wraps(func)
        def wrapper(self, query, *args, **kwargs):
            if hasattr(query, 'serialize'):
                query = query.serialize()

            assert isinstance(query, basestring), 'Expected query to be string'
            if self.debug:
                print('SQL:', query)

            return func(self, query, *args, **kwargs)
        return wrapper

    @serialize_query
    def execute(self, query, *args, **kwargs):
        return self.pool.execute(query, *args, **kwargs)

    @serialize_query
    def executemany(self, query, *args, **kwargs):
        return self.pool.executemany(query, *args, **kwargs)

    def executescript(self, sql):
        return self.pool.execute(sql, isolation_level=AUTOCOMMIT)

    @serialize_query
    def fetchone(self, query, *args, **kwargs):
        return self.pool.fetchone(query, *args, **kwargs)

    @serialize_query
    def fetchall(self, query, *args, **kwargs):
        return self.pool.fetchall(query, *args, **kwargs)

    @serialize_query
    def fetchiter(self, query, *args, **kwargs):
        return self.pool.fetchiter(query, *args, **kwargs)

    def transaction(self, *args, **kwargs):
        return self.pool.cursor(*args, **kwargs)

    def close(self):
        self.pool.closeall()

    @property
    def name(self):
        return self.kwargs.get('dbname')

    @staticmethod
    def command(host, port, dbname, user, password, maxsize, sql):
        pool = PostgresConnectionPool(host=host,
                                      port=port,
                                      dbname='postgres',
                                      user=user,
                                      password=password,
                                      maxsize=maxsize)
        pool.execute(sql, isolation_level=AUTOCOMMIT)

    @classmethod
    def create(cls, host, port, dbname, user, password, maxsize):
        sql = 'CREATE DATABASE {};'.format(dbname)
        cls.command(host, port, dbname, user, password, maxsize, sql)

    @classmethod
    def drop(cls, host, port, dbname, user, password, maxsize):
        sql = 'DROP DATABASE {};'.format(dbname)
        cls.command(host, port, dbname, user, password, maxsize, sql)

    def recreate(self):
        self.close()
        call_relevant_args(self.drop, self.connection_params)
        call_relevant_args(self.create, self.connection_params)
        #self.drop(**self.connection_params)
        #self.create(**self.connection_params)

    @classmethod
    def connect(cls, host, port, database, user, password,
                maxsize=DEFAULT_MAX_POOL_SIZE, debug=False):
        kwargs = dict(host=host,
                      port=port,
                      dbname=database,
                      user=user,
                      password=password,
                      maxsize=maxsize,
                      cursor_factory=NamedTupleCursor)
        pool = PostgresConnectionPool(**kwargs)
        try:
            conn = pool.create_connection()  # testing connection
        except psycopg2.OperationalError as exc:
            if 'does not exist' in str(exc):
                call_relevant_args(cls.create, kwargs)
            else:
                raise
        else:
            conn.close()  # close this connection as pool manages them later

        return cls(pool, kwargs, debug=debug)


class DatabaseContainer(dict):

    def __init__(self, databases):
        super(DatabaseContainer, self).__init__(databases)
        self.__dict__ = self
