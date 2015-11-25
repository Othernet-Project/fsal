"""
migrations.py: functions for managing migrations

Migration versions are tracked using a major and a minor version number. The
major version number is used to group migrations together, essentially allowing
to reset them back to 0. It was introduced in the later stages of development
when the need for resetting an existing database to an empty state came up, but
with the requirement to drop all the existing migrations for it as well.

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

import importlib
import logging
import os
import re
import sys

import psycopg2
import sqlize


PYMOD_RE = re.compile(r'^((\d{2})_(\d{2})_[^.]+)\.pyc?$', re.I)
VERSION_MULTIPLIER = 10000
MIGRATION_TABLE = 'migrations'
GET_VERSION_SQL = 'SELECT version FROM {table:s} WHERE id = 0;'.format(
    table=MIGRATION_TABLE
)
SET_VERSION_SQL = lambda version: sqlize.Replace(table=MIGRATION_TABLE,
                                                 constraints=('id',),
                                                 cols=('id', 'version'),
                                                 vals=('0', str(version)))
CREATE_MIGRATION_TABLE_SQL = """
CREATE TABLE {table:s}
(
    id serial primary key,
    version integer null
);
""".format(table=MIGRATION_TABLE)


def get_mods(package):
    """ List all loadable python modules in a directory

    This function looks inside the specified directory for all files that look
    like Python modules with a numeric prefix and returns them. It will omit
    any duplicates and return file names without extension.

    :param package: package object
    :returns:       list of tuples containing filename without extension,
                    major_version and minor_version
    """
    pkgdir = package.__path__[0]
    matches = filter(None, [PYMOD_RE.match(f) for f in os.listdir(pkgdir)])
    parse_match = lambda groups: (groups[0], int(groups[1]), int(groups[2]))
    return sorted(list(set([parse_match(m.groups()) for m in matches])),
                  key=lambda x: (x[1], x[2]))


def get_new(modules, min_major_version, min_minor_version):
    """ Get list of migrations that haven't been run yet

    :param modules:           iterable containing module names
    :param min_major_version: minimum major version
    :param min_minor_version: minimum minor version
    :returns:                 return an iterator that yields only items which
                              versions are >= min_ver
    """
    for mod_data in modules:
        (modname, mod_major_version, mod_minor_version) = mod_data
        if (mod_major_version > min_major_version or
                (mod_major_version == min_major_version and
                    mod_minor_version >= min_minor_version)):
            yield mod_data


def load_mod(module, package):
    """ Load a module named ``module`` from given search``path``

    The module path prefix is set according to the ``prefix`` argument.
    By defualt the module is loaded as if it comes from a global
    'db_migrations' package.  As such, it may conflict with any 'db_migration'
    package. The module can be looked up in ``sys.modules`` as
    ``db_migration.MODNAME`` where ``MODNAME`` is the name supplied as
    ``module`` argument. Keep in mind that relative imports from within the
    module depend on this prefix.

    This function raises an ``ImportError`` exception if module is not found.

    :param module:  name of the module to load
    :param package: package object
    :returns:       module object
    """
    name = '%s.%s' % (package.__name__, module)
    if name in sys.modules:
        return sys.modules[name]
    return importlib.import_module(name, package=package.__name__)


def pack_version(major_version, minor_version):
    """Pack the two version integers into one int."""
    return major_version * VERSION_MULTIPLIER + minor_version


def unpack_version(version):
    """Unpack a single version integer into the two major and minor
    components."""
    minor_version = version % VERSION_MULTIPLIER
    major_version = (version - minor_version) / VERSION_MULTIPLIER
    return (major_version, minor_version)


def recreate(db):
    db.recreate()
    db.executescript(CREATE_MIGRATION_TABLE_SQL)
    db.execute(SET_VERSION_SQL(0))
    return (0, 0)


def get_version(db):
    """ Query database and return migration version. WARNING: side effecting
    function! if no version information can be found, any existing database
    matching the passed one's name will be deleted and recreated.

    :param db:  connetion object
    :returns:   current migration version
    """
    try:
        result = db.fetchone(GET_VERSION_SQL)
    except psycopg2.ProgrammingError as exc:
        if 'does not exist' in str(exc):
            return recreate(db)
        raise
    else:
        if result is None:
            return recreate(db)
        version = result['version']
        return unpack_version(version)


def set_version(db, major_version, minor_version):
    """ Set database migration version

    :param db:             connetion object
    :param major_version:  integer major version of migration
    :param minor_version:  integer minor version of migration
    """
    version = pack_version(major_version, minor_version)
    db.execute(SET_VERSION_SQL(version))


def run_migration(major_version, minor_version, db, mod, conf={}):
    """ Run migration script

    :param major_version: major version number of the migration
    :param minor_version: minor version number of the migration
    :param db:            database connection object
    :param path:          path of the migration script
    :param conf:          application configuration (if any)
    """
    with db.transaction():
        mod.up(db, conf)
        set_version(db, major_version, minor_version)


def migrate(db, package, conf={}):
    """ Run all migrations that have not been run

    Migrations will be run inside a transaction.

    :param db:              database connection object
    :param package:         package that contains the migrations
    :param conf:            application configuration object
    """
    (current_major_version, current_minor_version) = get_version(db)
    package = importlib.import_module(package)
    logging.debug('Migration version for %s is %s.%s',
                  package.__name__,
                  current_major_version,
                  current_minor_version)
    mods = get_mods(package)
    migrations = get_new(mods,
                         current_major_version,
                         current_minor_version + 1)
    for (modname, major_version, minor_version) in migrations:
        mod = load_mod(modname, package)
        run_migration(major_version, minor_version, db, mod, conf)
        logging.debug("Finished migrating to %s", modname)
