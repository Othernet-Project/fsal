"""
databases.py: Database utility functions

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

import logging
import os

from . import squery

from .migrations import migrate
from ..system import ensure_dir


def get_database_path(conf, name):
    return os.path.join(conf['database.path'], name + '.sqlite')


def get_database_configs(conf):
    databases = dict()
    names = conf['database.names']
    for db_name in names:
        databases[db_name] = get_database_path(conf, db_name)
    return databases


def get_databases(config):
    database_configs = get_database_configs(config)
    for db_name, db_path in database_configs.items():
        logging.debug('Using database {}'.format(db_path))

    # Make sure all necessary directories are present
    for db_path in database_configs.values():
        ensure_dir(os.path.dirname(db_path))

    return squery.get_databases(database_configs)


def apply_migrations(config, context):
    for db_name, db in context['databases'].items():
        migrate(db,
                'fsal.migrations.{0}'.format(db_name),
                config)


def close_databases(databases):
    for db in databases.values():
        db.close()
