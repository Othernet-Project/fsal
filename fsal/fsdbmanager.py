import os
import re
import shutil
import logging
import time
from itertools import ifilter

from .utils import fnwalk, to_unicode
from .fs import File, Directory
try:
    from functools import lru_cache
except:
    from .utils import lru_cache


class FSDBManager(object):
    FILE_TYPE = 0
    DIR_TYPE = 1

    FS_TABLE = 'fsentries'
    STATS_TABLE = 'dbmgr_stats'

    ROOT_DIR_PATH = '.'

    def __init__(self, config, context):
        base_path = os.path.abspath(config['fsal.basepath'])
        if not os.path.isdir(base_path):
            raise RuntimeError('Invalid basepath: "%s"' % (base_path))

        self.base_path = base_path
        self.db = context['databases'].fs
        self.last_op_time = self._read_last_op_time()

    def start(self):
        self._refresh_db()

    def stop(self):
        pass

    def get_root_dir(self):
        d = Directory.from_path(self.base_path, '.')
        d.__id = 0
        return d

    def list_dir(self, path):
        d = self._get_dir(path)
        if d is None:
            return (False, [])
        else:
            q = self.db.Select('*', sets=self.FS_TABLE, where='parent_id = ?')
            cursor = self.db.query(q, d.__id)
            return (True, self._fso_row_iterator(cursor))

    def search(self, query, whole_words=False, exclude=None):
        is_match, files = self.list_dir(query)
        if is_match:
            result_gen = files
        else:
            like_pattern = '%s' if whole_words else '%%%s%%'
            like_words = [(like_pattern % k) for k in query.split()]
            q = self.db.Select('*', sets=self.FS_TABLE)
            for _ in like_words:
                if whole_words:
                    where_clause = 'name like ?'
                else:
                    where_clause = 'lower(name) like ?'
                q.where |= where_clause
            self.db.execute(q, like_words)
            result_gen = self._fso_row_iterator(self.db.cursor)

        if exclude and len(exclude) > 0:
            clean_exclude = [f.replace('.', '\.') for f in exclude]
            rxp_str = '|'.join(['^%s$' % f for f in clean_exclude])
            rxp = re.compile(rxp_str)
            result_gen = ifilter(lambda fso: rxp.match(fso.name) is None,
                                 result_gen)
        return (is_match, result_gen)

    def exists(self, path):
        return (self.get_fso(path) is not None)

    def is_dir(self, path):
        fso = self._get_dir(path)
        return (fso is not None)

    def is_file(self, path):
        fso = self._get_file(path)
        return (fso is not None)

    @lru_cache(maxsize=100)
    def get_fso(self, path):
        valid, path = self._validate_path(path)
        if not valid:
            return None
        if path == self.ROOT_DIR_PATH:
            return self.get_root_dir()
        else:
            q = self.db.Select('*', sets=self.FS_TABLE, where='path = ?')
            self.db.query(q, path)
            result = self.db.result
            return self._construct_fso(result) if result else None

    def remove(self, path):
        fso = self.get_fso(path)
        if fso is None:
            return (False, 'No such file or directory "%s"' % path)
        else:
            return self._remove_fso(fso)

    def _validate_path(self, path):
        if path is None:
            valid = False
        else:
            path = path.lstrip(os.sep)
            path = path.rstrip(os.sep)
            full_path = os.path.abspath(os.path.join(self.base_path, path))
            valid = full_path.startswith(self.base_path)
            path = os.path.relpath(full_path, self.base_path)
        return (valid, path)

    def _construct_fso(self, row):
        type = row.type
        cls = Directory if type == self.DIR_TYPE else File
        fso = cls.from_db_row(self.base_path, row)
        fso.__id = row.id
        return fso

    def _remove_fso(self, fso):
        remover = shutil.rmtree if fso.is_dir() else os.remove
        try:
            remover(fso.path)
            q = self.db.Delete(self.FS_TABLE, where='path LIKE ?')
            self.db.execute(q, ('%s%%' % (fso.rel_path),))
            logging.debug('Removing %d files/dirs' % (self.db.cursor.rowcount))
            self._record_op_time()
        except Exception as e:
            # FIXME: Handle exceptions more gracefully
            self._refresh_db()
            return (False, str(e))
        else:
            return (True, None)

    def _get_dir(self, path):
        fso = self.get_fso(path)
        return fso if fso and fso.is_dir() else None

    def _get_file(self, path):
        fso = self.get_fso(path)
        return fso if fso and fso.is_file() else None

    def _refresh_db(self):
        start = time.time()
        self._prune_db()
        self._update_db()
        end = time.time()
        logging.debug('DB refreshed in %0.3f ms' % ((end - start) * 1000))

    def _prune_db(self, batch_size=1000):
        with self.db.transaction():
            q = self.db.Select('path', sets=self.FS_TABLE)
            self.db.query(q)
            cursor = self.db.drop_cursor()
            removed_paths = []
            for result in cursor:
                path = result.path
                full_path = os.path.join(self.base_path, path)
                if not os.path.exists(full_path):
                    removed_paths.append(path)
                if len(removed_paths) >= batch_size:
                    self._remove_paths(removed_paths)
                    removed_paths = []
            if len(removed_paths) >= 0:
                self._remove_paths(removed_paths)

    def _remove_paths(self, paths):
        q = self.db.Delete(self.FS_TABLE, where='path = ?')
        self.db.executemany(q, ((p,) for p in paths))

    def _update_db(self):
        def checker(path):
            return (path != self.base_path and
                    os.path.getmtime(path) > self.last_op_time)

        with self.db.transaction():
            for path in fnwalk(self.base_path, checker):
                path = to_unicode(path)
                rel_path = os.path.relpath(path, self.base_path)
                if os.path.isdir(path):
                    fso = Directory.from_path(self.base_path, rel_path)
                else:
                    fso = File.from_path(self.base_path, rel_path)
                self._update_fso_entry(fso)
        self._record_op_time()

    def _update_fso_entry(self, fso):
        parent, name = os.path.split(fso.rel_path)
        parent_dir = self._get_dir(parent)
        parent_id = parent_dir.__id if parent_dir else 0

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
            yield self._construct_fso(result)

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
        self.last_op_time = time.time()
        q = self.db.Update(self.STATS_TABLE, op_time=':op_time')
        self.db.query(q, op_time=self.last_op_time)
