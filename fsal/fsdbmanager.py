import os
import shutil
import logging

from .utils import fnwalk
from .fs import File, Directory

import time


class FSDBManager(object):
    FILE_TYPE = 0
    DIR_TYPE = 1

    FS_TABLE = 'fsentries'
    STATS_TABLE = 'dbmgr_stats'

    ROOT_DIR_PATHS = ('.', os.sep)

    def __init__(self, config, context):
        base_path = os.path.abspath(config['fsal.basepath'])
        if not os.path.isdir(base_path):
            raise RuntimeError('Invalid basepath: "%s"' % (base_path))

        self.base_path = base_path
        self.db = context['databases'].fs
        self.last_op_time = self._read_last_op_time()
        self._root_dir = None

    def start(self):
        self._refresh_db()

    def stop(self):
        pass

    @property
    def root_dir(self):
        if self._root_dir is None:
            self._root_dir = Directory.from_path(self.base_path, '.')
            self._root_dir.id = 0
        return self._root_dir

    def list_dir(self, path):
        d = self._get_dir(path)
        if d is None:
            return (False, [])
        else:
            q = self.db.Select('*', sets=self.FS_TABLE, where='parent_id = ?')
            cursor = self.db.query(q, d.id)
            return (True, self._fso_row_iterator(cursor))

    def search(self, query):
        #TODO: Add support for whole_words
        success, files = self.list_dir(query)
        if success:
            return (success, files)
        else:
            keywords = ["%%%s%%" % k for k in query.split()]
            q = self.db.Select('*', sets=self.FS_TABLE)
            for _ in keywords:
                q.where |= 'name LIKE ?'
            self.db.execute(q, keywords)
            return (False, self._fso_row_iterator(self.db.cursor))

    def exists(self, path):
        return (self.get_fso(path) is not None)

    def is_dir(self, path):
        fso = self.get_fso(path)
        return (fso is not None and fso.is_dir())

    def is_file(self, path):
        fso = self.get_fso(path)
        return (fso is not None and fso.is_file())

    def remove(self, path):
        fso = self._get_fso(path)
        if fso is None:
            return (False, 'No such file or directory "%s"' % path)
        else:
            return self._remove_fso(fso)

    def _is_valid_path(self, path):
        if path is None:
            return False
        path = path.lstrip(os.sep)
        full_path = os.path.abspath(os.path.join(self.base_path, path))
        return full_path.startswith(self.base_path)

    def _get_fso(self, path):
        if not self._is_valid_path(path):
            return None

        if path in self.ROOT_DIR_PATHS:
            return self.root_dir
        else:
            q = self.db.Select('*', sets=self.FS_TABLE, where='path = ?')
            self.db.query(q, path)
            result = self.db.result
            if result:
                return self._make_fso(result)
            else:
                return None

    def _make_fso(self, row):
        type = row.type
        cls = Directory if type == self.DIR_TYPE else File
        fso = cls.from_db_row(self.base_path, row)
        fso._id = row.id
        return fso

    def _remove_fso(self, fso):
        remover = shutil.rmtree if fso.is_dir() else os.remove
        try:
            remover(fso.path)
            q = self.db.Delete(self.FS_TABLE, where = 'path LIKE ?')
            self.db.execute(q, ('%s%%'%(fso.rel_path),))
            logging.debug("Removing %d files/folders" %(self.db.cursor.rowcount))
        except Exception as e:
            #FIXME: Handle error more gracefully
            self._refresh_db()
            return (False, str(e))
        else:
            return (True, None)

    def _get_dir(self, path):
        fso = self._get_fso(path)
        return fso if fso and fso.is_dir() else None

    def _refresh_db(self):
        self._prune_db()
        self._update_db()

    def _prune_db(self, batch_size=1000):
        with self.db.transaction():
            q = self.db.Select('path', sets=self.FS_TABLE)
            #FIXME: Batch the results and delete selectively
            results = self.db.query(q).fetchall()
            for result in results:
                path = result.path
                full_path = os.path.join(self.base_path, path)
                if not os.path.exists(full_path):
                    q2 = self.db.Delete(self.FS_TABLE, where='path = ?')
                    self.db.query(q2, path)
                    logging.debug("Removing db entry for %s" % path)

    def _update_db(self):
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
                self._update_entry(fso)
        self._record_op_time()

    def _update_entry(self, fso):
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
            yield self._make_fso(result)

    def _read_last_op_time(self):
        q = self.db.Select('op_time', sets=self.STATS_TABLE)
        self.db.query(q)
        op_time = self.db.result.op_time
        # If the recorded op_time is greater than current time, assume
        # system time was modified and revert to epoch time
        if op_time > time.time():
            op_time = 0.0
        return op_time

    def _record_op_time(self):
        q = self.db.Update(self.STATS_TABLE, op_time=':op_time')
        self.db.query(q, op_time=time.time())
