"""
databases.py: Database utility functions

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from squery_pg.migrations import migrate
from squery_pg.squery_pg import Database, DatabaseContainer


def get_databases(db_name, host, port, user, password, debug=False):
    databases = { db_name: Database.connect(host,
                                            port,
                                            db_name,
                                            user,
                                            password,
                                            debug=debug) }
    return DatabaseContainer(databases)


def init_databases(config):
    databases = get_databases(config['database.name'],
                              config['database.host'],
                              config['database.port'],
                              config['database.user'],
                              config['database.password'],
                              debug=False)
    # Run migrations on all databases
    for name, db in databases.items():
        migration_pkg = 'fsal.migrations.{0}'.format(name)
        migrate(db, migration_pkg, config)

    return databases


def close_databases(databases):
    for db in databases.values():
        db.close()
