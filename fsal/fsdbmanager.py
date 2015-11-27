import os
import re
import asyncfs
import shutil
import logging
import time
import collections
from itertools import ifilter

import gevent.queue
import scandir

from .utils import to_unicode, to_bytes
from .fs import File, Directory
from .ondd import ONDDNotificationListener
from .events import FileCreatedEvent, FileDeletedEvent, FileModifiedEvent, \
    DirCreatedEvent, DirModifiedEvent, DirDeletedEvent, FileSystemEventQueue

from librarian_core.contrib.tasks.scheduler import TaskScheduler


SQL_ESCAPE_CHAR = '\\'
SQL_WILDCARDS = [('_', SQL_ESCAPE_CHAR + '_'),
                 ('%', SQL_ESCAPE_CHAR + '%')]


def sql_escape_path(path):
    for char, escaped_char in SQL_WILDCARDS:
        path = path.replace(char, escaped_char)
    return path


def yielding_checked_fnwalk(path, fn, sleep_interval=0.01):
    try:
        parent, name = os.path.split(path)
        entry = scandir.GenericDirEntry(parent, name)
        if fn(entry):
            yield entry

        queue = gevent.queue.LifoQueue()
        queue.put(path)
        while True:
            try:
                path = queue.get(timeout=0)
            except gevent.queue.Empty:
                break
            else:
                for entry in scandir.scandir(path):
                    if fn(entry):
                        if entry.is_dir():
                            queue.put(entry.path)
                        yield entry
                gevent.sleep(sleep_interval)
    except Exception as e:
        logging.exception('Exception while directory walking: {}'.format(str(e)))


class FSDBManager(object):
    FILE_TYPE = 0
    DIR_TYPE = 1

    FS_TABLE = 'fsentries'
    STATS_TABLE = 'dbmgr_stats'

    ROOT_DIR_PATH = '.'

    PATH_LEN_LIMIT = 32767

    SLEEP_INTERVAL = 0.500

    def __init__(self, config, context):
        base_path = os.path.abspath(config['fsal.basepath'])
        if not os.path.isdir(base_path):
            raise RuntimeError('Invalid basepath: "%s"' % (base_path))

        self.base_path = base_path
        self.db = context['databases'].fs
        blacklist = config['fsal.blacklist']
        sanitized_blacklist = []
        for p in blacklist:
            valid, p = self._validate_path(p)
            if valid:
                sanitized_blacklist.append(p)
        self.blacklist = sanitized_blacklist

        self.notification_listener = ONDDNotificationListener(config, self._handle_notification)
        self.event_queue = FileSystemEventQueue(config, context)
        self.scheduler = TaskScheduler(0.2)

    def start(self):
        self.notification_listener.start()
        self._refresh_db_async()

    def stop(self):
        self.notification_listener.stop()

    def get_root_dir(self):
        try:
            d = Directory.from_path(self.base_path, '.')
            d.__id = 0
            return d
        except OSError:
            return None

    def list_dir(self, path):
        d = self._get_dir(path)
        if d is None:
            return (False, [])
        else:
            q = self.db.Select('*', sets=self.FS_TABLE, where='parent_id = %s')
            row_iter = self.db.fetchiter(q, (d.__id,))
            return (True, self._fso_row_iterator(row_iter))

    def search(self, query, whole_words=False, exclude=None):
        is_match, files = self.list_dir(query)
        if is_match:
            result_gen = files
        else:
            like_pattern = '%s' if whole_words else '%%%s%%'
            words = map(sql_escape_path, query.split())
            like_words = [(like_pattern % w) for w in words]
            q = self.db.Select('*', sets=self.FS_TABLE)
            for _ in like_words:
                if whole_words:
                    where_clause = 'name LIKE %s'
                else:
                    where_clause = 'lower(name) LIKE %s'
                where_clause += ' ESCAPE \'{}\''.format(SQL_ESCAPE_CHAR)
                q.where |= where_clause
            row_iter = self.db.fetchiter(q, like_words)
            result_gen = self._fso_row_iterator(row_iter)

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

    def get_fso(self, path):
        valid, path = self._validate_path(path)
        if not valid:
            return None
        if path == self.ROOT_DIR_PATH:
            return self.get_root_dir()
        else:
            q = self.db.Select('*', sets=self.FS_TABLE, where='path = %s')
            result = self.db.fetchone(q, (path,))
            return self._construct_fso(result) if result else None

    def remove(self, path):
        fso = self.get_fso(path)
        if fso is None:
            return (False, 'No such file or directory "%s"' % path)
        else:
            return self._remove_fso(fso)

    def transfer(self, src, dest):
        success, msg = self._validate_transfer(src, dest)
        if not success:
            return (success, msg)

        abs_src = os.path.abspath(src)
        abs_dest = os.path.abspath(os.path.join(self.base_path, dest))
        logging.debug('Transferring content from "%s" to "%s"' % (abs_src,
                                                                  abs_dest))
        real_dst = abs_dest
        if os.path.isdir(real_dst):
            real_dst = os.path.join(real_dst, asyncfs.basename(abs_src))
        try:
            asyncfs.move(abs_src, abs_dest)
        except (asyncfs.Error, IOError) as e:
            logging.error('Error while transfering content: %s' % str(e))
            success = False
            msg = str(e)

        # Find the deepest parent in hierarchy which has been indexed
        path = os.path.relpath(real_dst, self.base_path)
        path = self._deepest_indexed_parent(path)
        logging.debug('Indexing %s' % path)
        self._update_db_async(path)
        return (success, msg)

    def get_changes(self, limit=100):
        return self.event_queue.getitems(limit)

    def confirm_changes(self, limit=100):
        return self.event_queue.delitems(limit)

    def refresh_path(self, path):
        valid, path = self._validate_path(path)
        if not valid:
            return (False, ('No such file or directory "%s"' % path))
        self._update_db_async(path)
        return (True, None)

    def _handle_notification(self, notification):
        path = notification.path
        logging.debug("Notification received for %s" % path)
        # Find the deepest parent in hierarchy which has been indexed
        while path != '':
            if not self.exists(path):
                path = os.path.dirname(path)
            else:
                break
        if path == '':
            logging.warn("Cannot index path %s" % notification.path)
            return
        path = self._deepest_indexed_parent(path)
        self._update_db_async(path)

    def _validate_path(self, path):
        if path is None or len(path.strip()) == 0:
            valid = False
        else:
            path = path.strip()
            path = path.lstrip(os.sep)
            path = path.rstrip(os.sep)
            full_path = os.path.abspath(os.path.join(self.base_path, path))
            valid = full_path.startswith(self.base_path)
            path = os.path.relpath(full_path, self.base_path)
        return (valid, path)

    def _validate_external_path(self, path):
        if path is None or len(path.strip()) == 0:
            return (False, path)
        else:
            path = path.strip()
            path = path.rstrip(os.sep)
            full_path = os.path.abspath(path)
            return (True, full_path)

    def _deepest_indexed_parent(self, path):
        while path != '':
            parent = os.path.dirname(path)
            if self.exists(parent):
                break
            path = parent
        if path == '':
            path = self.ROOT_DIR_PATH
        return path

    def _is_blacklisted(self, path):
        return any([path.startswith(p) for p in self.blacklist])

    def _construct_fso(self, row):
        type = row['type']
        cls = Directory if type == self.DIR_TYPE else File
        fso = cls.from_db_row(self.base_path, row)
        fso.__id = row['id']
        return fso

    def _remove_from_fs(self, fso):
        events = []
        for entry in yielding_checked_fnwalk(fso.path, lambda: True):
            path = entry.path
            rel_path = os.path.relpath(path, self.base_path)
            if entry.is_dir():
                event = DirDeletedEvent(rel_path)
            else:
                event = FileDeletedEvent(rel_path)
            events.append(event)
        remover = shutil.rmtree if fso.is_dir() else os.remove
        remover(fso.path)
        return events

    def _remove_fso(self, fso):
        try:
            events = self._remove_from_fs(fso)
            path = sql_escape_path(fso.rel_path)
            q = self.db.Delete(self.FS_TABLE)
            q.where = 'path LIKE %s ESCAPE \'{}\''.format(SQL_ESCAPE_CHAR)
            if fso.is_dir():
                pattern = '%s' + os.sep + '%%'
                self.db.executemany(q, (((pattern % path),), (path,)))
            else:
                self.db.execute(q, (path,))
            self.event_queue.additems(events)
            logging.debug('Removing %d files/dirs' % (self.db.cursor.rowcount))
        except Exception as e:
            msg = 'Exception while removing "%s": %s' % (fso.rel_path, str(e))
            logging.error(msg)
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

    def _validate_transfer(self, src, dest):
        src_valid, abs_src = self._validate_external_path(src)
        dest_valid, dest = self._validate_path(dest)
        if not src_valid or not os.path.exists(abs_src) or self.exists(src):
            return (False, u'Invalid transfer source directory %s' % src)
        if not dest_valid:
            return (False, u'Invalid transfer destination directory %s' % dest)

        abs_dest = os.path.abspath(os.path.join(self.base_path, dest))
        real_dst = abs_dest
        if os.path.isdir(abs_dest):
            real_dst = os.path.join(abs_dest, asyncfs.basename(abs_src))
            if os.path.exists(real_dst):
                return (False,
                        'Destination path "%s" already exists' % real_dst)

        for entry in yielding_checked_fnwalk(abs_src, lambda p: True):
            path = entry.path
            path = os.path.relpath(path, abs_src)
            dest_path = os.path.abspath(os.path.join(real_dst, path))
            if len(to_bytes(dest_path)) > self.PATH_LEN_LIMIT:
                msg = '%s exceeds path length limit' % dest_path
                return (False, msg)

        return (True, None)

    def _refresh_db_async(self):
        self.scheduler.schedule(self._refresh_db)

    def _refresh_db(self):
        start = time.time()
        self._prune_db()
        self._update_db()
        end = time.time()
        logging.debug('DB refreshed in %0.3f ms' % ((end - start) * 1000))

    def _prune_db(self, batch_size=1000):
        q = self.db.Select('path', sets=self.FS_TABLE)
        removed_paths = []
        for result in self.db.fetchiter(q):
            path = result['path']
            full_path = os.path.join(self.base_path, path)
            if not os.path.exists(full_path) or self._is_blacklisted(path):
                logging.debug('Removing db entry for "%s"' % path)
                removed_paths.append(path)
            if len(removed_paths) >= batch_size:
                self._remove_paths(removed_paths)
                removed_paths = []
        if len(removed_paths) >= 0:
            self._remove_paths(removed_paths)

    def _remove_paths(self, paths):
        q = self.db.Delete(self.FS_TABLE, where='path = %s')
        self.db.executemany(q, ((p,) for p in paths))
        events = []
        for p in paths:
            abs_p = os.path.abspath(os.path.join(self.base_path, p))
            if os.path.isdir(abs_p):
                event = DirDeletedEvent(p)
            else:
                event = FileDeletedEvent(p)
            events.append(event)
        self.event_queue.additems(events)

    def _update_db_async(self, src_path=ROOT_DIR_PATH):
        self.scheduler.schedule(self._update_db, args=(src_path,))

    def _update_db(self, src_path=ROOT_DIR_PATH):
        def checker(entry):
            path = entry.path
            result = (path != self.base_path and not entry.is_symlink())
            rel_path = os.path.relpath(path, self.base_path)
            result = result and not self._is_blacklisted(rel_path)
            return result

        src_path = os.path.abspath(os.path.join(self.base_path, src_path))
        src_path = to_unicode(src_path)
        if not os.path.exists(src_path):
            logging.error('Cannot index "%s". Path does not exist' % src_path)
            return
        id_cache = FIFOCache(1024)
        try:
            for entry in yielding_checked_fnwalk(src_path, checker):
                path = entry.path
                rel_path = os.path.relpath(path, self.base_path)
                parent_path = os.path.dirname(rel_path)
                parent_id = id_cache[parent_path] if parent_path in id_cache else None
                if entry.is_dir():
                    fso = Directory.from_stat(self.base_path, rel_path, entry.stat())
                else:
                    fso = File.from_stat(self.base_path, rel_path, entry.stat())
                old_fso = self.get_fso(rel_path)
                if not old_fso:
                    event_cls = DirCreatedEvent if fso.is_dir() else FileCreatedEvent
                elif old_fso != fso:
                    event_cls = DirModifiedEvent if fso.is_dir() else FileModifiedEvent
                if not old_fso or old_fso != fso:
                    if event_cls:
                        self.event_queue.add(event_cls(rel_path))
                    fso_id = self._update_fso_entry(fso, parent_id, old_fso)
                    logging.debug('Updating db entry for "%s"' % rel_path)
                    if fso.is_dir():
                        id_cache[fso.rel_path] = fso_id
        except Exception:
            logging.exception('Exception while indexing "%s"' % src_path)


    def _update_fso_entry(self, fso, parent_id=None, old_entry=None):
        if not parent_id:
            parent, name = os.path.split(fso.rel_path)
            parent_dir = self._get_dir(parent)
            parent_id = parent_dir.__id if parent_dir else 0

        vals = {
            'parent_id': parent_id,
            'type': self.DIR_TYPE if fso.is_dir() else self.FILE_TYPE,
            'name': fso.name,
            'size': fso.size,
            'create_time': fso.create_date,
            'modify_time': fso.modify_date,
            'path': fso.rel_path
        }

        if old_entry:
            q = self.db.Update(self.FS_TABLE, 'id = %s')
            q.set_args = vals
            self.db.execute(q, (old_entry.__id,))
            return old_entry.__id
        else:
            cols = ['parent_id', 'type', 'name', 'size', 'create_time',
                    'modify_time', 'path']
            q = self.db.Insert(self.FS_TABLE, cols=cols)
            raw_query = '{} RETURNING id;'.format(q.serialize()[:-1])
            result = self.db.fetchone(raw_query, vals)
            return result['id']

    def _clear_db(self):
        with self.db.transaction():
            q = self.db.Delete(self.FS_TABLE)
            self.db.execute(q)

    def _fso_row_iterator(self, cursor):
        for result in cursor:
            yield self._construct_fso(result)


class FIFOCache(object):

    def __init__(self, maxsize):
        self.maxsize = maxsize
        self.cache = collections.OrderedDict()

    def __contains__(self, key):
        return (key in self.cache)

    def __getitem__(self, key):
        try:
            return self.cache[key]
        except KeyError:
            return None

    def __setitem__(self, key, value):
        if len(self.cache) >= self.maxsize:
            self.cache.popitem(False)
        self.cache[key] = value
