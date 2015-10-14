import os

import scandir

from .fs import File, Directory


FILE_TYPE = 0
DIR_TYPE = 1
TABLE_NAME = 'fsentries'


class FSDBManager(object):
    def __init__(self, config, databases):
        self.config = config
        self.db = databases.fs

    def start(self):
        self._refresh_db()

    def _refresh_db(self):
        base_path = self.config['fsal.basepath']
        self._clear_db()
        self._populate_db(base_path, base_path)

    def _clear_db(self):
        with self.db.transaction():
            q = self.db.Delete(TABLE_NAME)
            self.db.execute(q)

    def _add_to_db(self, fso, ftype, parent_id):
        q = self.db.Insert(TABLE_NAME,
                      cols=('parent_id', 'type', 'name', 'size', 'create_time',
                            'modify_time', 'path'))
        size = fso.size if hasattr(fso, 'size') else 0
        values = (parent_id, ftype, fso.name, size, fso.create_date,
                  fso.modify_date, fso.rel_path)
        self.db.execute(q, values)
        return self.db.cursor.lastrowid

    def _get_from_db(self, path):
        pass

    def _populate_db(self, base_path, path, parent_id=0):
        if not os.path.isdir(path):
            return

        try:
            scandir.scandir(path)
        except OSError:
            return

        dir_id = parent_id
        if path != base_path:
            rel_path = os.path.relpath(path, base_path)
            dir = Directory.from_path(base_path, rel_path)
            dir_id = self._add_to_db(dir, DIR_TYPE, parent_id)

        for entry in scandir.scandir(path):
            rel_path = os.path.relpath(entry.path, base_path)
            if entry.is_dir(follow_symlinks=False):
                self._populate_db(base_path, entry.path, dir_id)
            else:
                f = File.from_path(base_path, rel_path)
                self._add_to_db(f, FILE_TYPE, dir_id)

