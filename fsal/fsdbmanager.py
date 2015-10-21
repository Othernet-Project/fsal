import os
import logging

from .utils import fnwalk
from .fs import File, Directory

import time


class FSDBManager(object):
    FILE_TYPE = 0
    DIR_TYPE = 1

    FS_TABLE = 'fsentries'
    STATS_TABLE = 'dbmgr_stats'

    def __init__(self, config, context):
        self.base_path = os.path.abspath(config['fsal.basepath'])
        if not os.path.isdir(self.base_path):
            raise RuntimeError('Invalid basepath: "%s"' % (self.base_path))

        self.db = context['databases'].fs
        self.last_op_time = self._read_last_op_time()

    def start(self):
        self.refresh_db()

    def stop(self):
        pass

    def is_valid_path(self, path):
        path = path.lstrip(os.sep)
        full_path = os.path.abspath(os.path.join(self.base_path, path))
        return full_path.startswith(self.base_path)

    def list_dir(self, path):
        if not self.is_valid_path(path):
            return (False, [])

        if path == '.':
            parent_id = 0
        else:
            q = self.db.Select('id', sets=self.FS_TABLE, where='path = ?')
            self.db.query(q, path)
            result = self.db.result
            if not result:
                return (False, [])
            else:
                parent_id = result.id

        q = self.db.Select('*', sets=self.FS_TABLE, where='parent_id = ?')
        cursor = self.db.query(q, parent_id)
        return (True, self._fso_row_iterator(cursor))

    def search(self, query):
        if self.is_valid_path(query):
            success, files = self.list_dir(query)
            if success:
                return (success, files)

        keywords = ["%%%s%%" % k for k in query.split()]
        q = self.db.Select('*', sets=self.FS_TABLE)
        for _ in keywords:
            q.where |= 'name LIKE ?'

        self.db.execute(q, keywords)
        return (False, self._fso_row_iterator(self.db.cursor))

    def exists(self, path):
        if not self.is_valid_path(path):
            return False
        if path in (os.sep, os.curdir):
            return True
        else:
            q = self.db.Select('id', sets=self.FS_TABLE, where='path = ?',
                               limit=1)
            self.db.query(q, path)
            return self.db.result is not None

    def is_dir(self, path):
        if not self.is_valid_path(path):
            return False
        if path in (os.sep, os.curdir):
            return True
        else:
            q = self.db.Select('type', sets=self.FS_TABLE, where='path = ?',
                               limit=1)
            self.db.query(q, path)
            result = self.db.result
            return (result and result.type == self.DIR_TYPE)

    def is_file(self, path):
        if not self.is_valid_path(path):
            return False
        if path in (os.sep, os.curdir):
            return True
        else:
            q = self.db.Select('type', sets=self.FS_TABLE, where='path = ?',
                               limit=1)
            self.db.query(q, path)
            result = self.db.result
            return (result and result.type == self.FILE_TYPE)

    def refresh_db(self):
        self.prune_db()
        self.update_db()

    def prune_db(self):
        with self.db.transaction():
            q = self.db.Select('path', sets=self.FS_TABLE)
            for result in self.db.query(q):
                path = result.path
                full_path = os.path.join(self.base_path, path)
                if not os.path.exists(full_path):
                    q2 = self.db.Delete(self.FS_TABLE, where='path = ?')
                    self.db.query(q2, path)
                    logging.debug("Removing db entry for %s" % path)

    def update_db(self):
        def checker(path):
            return (path != self.base_path and
                    os.path.getmtime(path) > self.last_op_time)

        with self.db.transaction():
            for path in fnwalk(self.base_path, checker):
                rel_path = os.path.relpath(path, self.base_path)
                logging.debug("Updating db entry for %s" % rel_path)
                if os.path.isdir(path):
                    fso = Directory.from_path(self.base_path, rel_path)
                else:
                    fso = File.from_path(self.base_path, rel_path)
                self.update_entry(fso)
        self._record_op_time()

    def update_entry(self, fso):
        parent, name = os.path.split(fso.rel_path)
        q = self.db.Select('id', sets=self.FS_TABLE, where='path = ?')
        self.db.query(q, parent)
        parent_id = 0
        result = self.db.result
        if result:
            parent_id = result.id

        cols = ['parent_id', 'type', 'name', 'size', 'create_time',
                'modify_time', 'path']
        q = self.db.Replace(self.FS_TABLE, cols=cols)
        size = fso.size if hasattr(fso, 'size') else 0
        type = self.DIR_TYPE if fso.is_dir() else self.FILE_TYPE
        values = [parent_id, type, fso.name, size, fso.create_date,
                  fso.modify_date, fso.rel_path]
        self.db.execute(q, values)

    def _clear_db(self):
        with self.db.transaction():
            q = self.db.Delete(self.FS_TABLE)
            self.db.execute(q)

    def _fso_row_iterator(self, cursor):
        for result in cursor:
            type = result.type
            cls = Directory if type == self.DIR_TYPE else File
            yield cls.from_db_row(self.base_path, result)

    def _read_last_op_time(self):
        q = self.db.Select('op_time', sets=self.STAT_TABLE)
        self.db.query(q)
        op_time = self.db.result.op_time
        # If the recorded op_time is greater than current time, assume system
        # time was modified and revert to epoch time
        if op_time > time.time():
            op_time = 0.0
        return op_time

    def _record_op_time(self):
        q = self.db.Update(self.STAT_TABLE, op_time=':op_time')
        self.db.query(q, op_time=time.time())
