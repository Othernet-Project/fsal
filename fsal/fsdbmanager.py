import os
import re
import asyncfs
import shutil
import logging
import time
import collections
import functools
from itertools import ifilter

import gevent.queue
import scandir

from .utils import to_unicode, to_bytes, common_ancestor
from .fs import File, Directory
from .ondd import ONDDNotificationListener
from .bundles import BundleExtracter, abs_bundle_path
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
        if entry.is_dir():
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
        logging.exception(
            'Exception while directory walking: {}'.format(str(e)))


class FSDBManager(object):
    FILE_TYPE = 0
    DIR_TYPE = 1

    FS_TABLE = 'fsentries'
    STATS_TABLE = 'dbmgr_stats'

    ROOT_DIR_PATH = '.'

    PATH_LEN_LIMIT = 32767

    SLEEP_INTERVAL = 0.500

    def __init__(self, config, context):
        paths = config['fsal.basepaths']
        base_paths = map(os.path.abspath, filter(os.path.isdir, paths))
        if not base_paths:
            msg = 'No valid path found in basepaths: {}'.format(
                ', '.join(paths))
            raise RuntimeError(msg)

        self.base_paths = base_paths
        logging.debug('Using basepaths: {}'.format(', '.join(base_paths)))
        self.db = context['databases'].fs
        self.bundles_dir = config['bundles.bundles_dir']
        self.bundle_ext = BundleExtracter(config)

        blacklist = list()
        blacklist.append(config['fsal.blacklist'])
        blacklist.append(self.bundle_ext.bundles_dir)
        self.blacklist = blacklist

        self.notification_listener = ONDDNotificationListener(
            config, self._handle_notifications)
        self.event_queue = FileSystemEventQueue(config, context)
        self.scheduler = TaskScheduler(0.2)

    @property
    def blacklist(self):
        return self.__blacklist

    @blacklist.setter
    def blacklist(self, blacklist):
        self.__blacklist = [pattern for pattern in blacklist if pattern]
        self.__blacklist_rx = [re.compile(
            pattern, re.IGNORECASE) for pattern in self.__blacklist]

    def start(self):
        self.notification_listener.start()
        self._refresh_db_async()

    def stop(self):
        self.notification_listener.stop()

    def get_root_dir(self):
        try:
            # Root dir is a empty directory used only for maintaining the id 0
            d = Directory.from_path(self.base_paths[0], self.ROOT_DIR_PATH)
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
                    where_clause = 'name ILIKE %s'
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

    def exists(self, path, unindexed=False):
        if unindexed:
            valid, path = self._validate_path(path)
            if not valid:
                return False
            else:
                for base_path in self.base_paths:
                    full_path = os.path.abspath(os.path.join(base_path,
                                                             path))
                    if os.path.exists(full_path):
                        return True
                else:
                    return False
        else:
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
        # Assume that the last mentioned path is expected destination
        base_path = self.base_paths[-1]
        abs_dest = os.path.abspath(os.path.join(base_path, dest))
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
        path = os.path.relpath(real_dst, base_path)
        path = self._deepest_indexed_parent(path)
        logging.debug('Indexing %s' % path)
        self._update_db_async(path)
        return (success, msg)

    def get_changes(self, limit=100):
        return self.event_queue.getitems(limit)

    def confirm_changes(self, limit=100):
        return self.event_queue.delitems(limit)

    def refresh(self):
        self._refresh_db_async()

    def refresh_path(self, path):
        valid, path = self._validate_path(path)
        if not valid:
            return (False, ('No such file or directory "%s"' % path))
        self._update_db_async(path)
        return (True, None)

    def _handle_notifications(self, notifications):
        for notification in notifications:
            try:
                path = notification['path']
                logging.debug("Notification received for %s" % path)
                is_bundle, base_path = self._is_bundle(path)
                if is_bundle:
                    extracted_path = self._handle_bundle(base_path, path)
                    if not extracted_path:
                        logging.warn(
                            'Could not process bundle {}. Skipping...'.format(path))
                        continue
                    path = extracted_path
                # Find the deepest parent in hierarchy which has been indexed
                path = self._deepest_indexed_parent(path)
                if path == '':
                    logging.warn("Cannot index path %s" % notification.path)
                    return
                self._update_db_async(path)
            except:
                logging.exception('Unexpected error in handling notification')

    def _is_bundle(self, path):
        for base_path in self.base_paths:
            if self.bundle_ext.is_bundle(base_path, path):
                return (True, base_path)
        else:
            return (False, None)

    def _handle_bundle(self, base_path, path):
        success, paths = self.bundle_ext.extract_bundle(path, base_path)
        if success:
            try:
                abspath = abs_bundle_path(base_path, path)
                os.remove(abspath)
            except OSError as e:
                logging.exception(
                    'Exception while removing bundle after extraction: {}'.format(str(e)))
            return common_ancestor(*paths)
        return None

    def _validate_path(self, path):
        if path is None or len(path.strip()) == 0:
            valid = False
        else:
            path = path.strip()
            path = path.lstrip(os.sep)
            path = path.rstrip(os.sep)
            # This checks for paths which escape the base path, so test against
            # any path is valid
            base_path = self.base_paths[0]
            full_path = os.path.abspath(os.path.join(base_path, path))
            valid = full_path.startswith(base_path)
            path = os.path.relpath(full_path, base_path)
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
            if parent == '':
                parent = self.ROOT_DIR_PATH
            if self.exists(parent):
                break
            path = parent
        if path == '':
            path = self.ROOT_DIR_PATH
        return path

    def _is_blacklisted(self, path):
        # match() is used to ensure matches start from beginning of the path
        return any((p.match(path) for p in self.__blacklist_rx))

    def _construct_fso(self, row):
        type = row['type']
        cls = Directory if type == self.DIR_TYPE else File
        fso = cls.from_db_row(row)
        fso.__id = row['id']
        return fso

    def _remove_from_fs(self, fso):
        events = []
        if os.path.isdir(fso.path):
            checker = functools.partial(self._fnwalk_checker, fso.base_path)
            for entry in yielding_checked_fnwalk(fso.path, checker):
                path = entry.path
                rel_path = os.path.relpath(path, fso.base_path)
                if entry.is_dir():
                    event = DirDeletedEvent(rel_path)
                else:
                    event = FileDeletedEvent(rel_path)
                events.append(event)
        else:
            events.append(FileDeletedEvent(os.path.relpath(fso.path,
                                                           fso.base_path)))
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
                count = self.db.executemany(q, (((pattern % path),), (path,)))
            else:
                count = self.db.execute(q, (path,))
            self.event_queue.additems(events)
            logging.debug('Removing %d files/dirs' % (count))
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

        base_path = self.base_paths[-1]
        abs_dest = os.path.abspath(os.path.join(base_path, dest))
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
        self._extract_bundles()
        self._update_db()
        end = time.time()
        logging.debug('DB refreshed in %0.3f ms' % ((end - start) * 1000))

    def _prune_db(self, batch_size=1000):
        q = self.db.Select('base_path, path', sets=self.FS_TABLE)
        removed_paths = []
        for result in self.db.fetchiter(q):
            path = result['path']
            base_path = result['base_path'] or ''
            full_path = os.path.join(base_path, path)
            if base_path not in self.base_paths or not os.path.exists(full_path) or self._is_blacklisted(path):
                logging.debug('Removing db entry for "%s"' % path)
                removed_paths.append((base_path, path))
            if len(removed_paths) >= batch_size:
                self._remove_paths(removed_paths)
                removed_paths = []
        if len(removed_paths) >= 0:
            self._remove_paths(removed_paths)

    def _remove_paths(self, paths):
        q = self.db.Delete(self.FS_TABLE, where='path = %s')
        self.db.executemany(q, ((p,) for _, p in paths))
        events = []
        for b, p in paths:
            abs_p = os.path.abspath(os.path.join(b, p))
            if os.path.isdir(abs_p):
                event = DirDeletedEvent(p)
            else:
                event = FileDeletedEvent(p)
            events.append(event)
        self.event_queue.additems(events)

    def _update_db_async(self, src_path=ROOT_DIR_PATH):
        self.scheduler.schedule(self._update_db, args=(src_path,))

    def _fnwalk_checker(self, base_path, entry):
        path = entry.path
        result = (path not in self.base_paths and not entry.is_symlink())
        rel_path = os.path.relpath(path, base_path)
        result = result and not self._is_blacklisted(rel_path)
        return result

    def _update_db(self, src_path=ROOT_DIR_PATH):
        for base_path in self.base_paths:
            abspath = os.path.abspath(os.path.join(base_path, src_path))
            if os.path.exists(abspath):
                self._update_db_for_basepath(base_path, src_path)

    def _update_db_for_basepath(self, base_path, src_path):
        src_path = os.path.abspath(os.path.join(base_path, src_path))
        src_path = to_unicode(src_path)
        if not os.path.exists(src_path):
            logging.error('Cannot index "%s". Path does not exist' % src_path)
            return
        id_cache = FIFOCache(1024)
        try:
            checker = functools.partial(self._fnwalk_checker, base_path)
            for entry in yielding_checked_fnwalk(src_path, checker):
                path = entry.path
                rel_path = os.path.relpath(path, base_path)
                parent_path = os.path.dirname(rel_path)
                parent_id = id_cache[parent_path] if parent_path in id_cache else None
                if entry.is_dir():
                    fso = Directory.from_stat(
                        base_path, rel_path, entry.stat())
                else:
                    fso = File.from_stat(base_path, rel_path, entry.stat())
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

    def _extract_bundles(self):
        def bundle_checker(base_path, entry):
            path = os.path.relpath(entry.path, base_path)
            is_bundle, _ = self._is_bundle(path)
            return is_bundle

        for base_path in self.base_paths:
            try:
                path = os.path.abspath(
                    os.path.join(base_path, self.bundles_dir))
                checker = functools.partial(bundle_checker, base_path)
                for entry in yielding_checked_fnwalk(path, checker):
                    try:
                        path = os.path.relpath(entry.path, base_path)
                        logging.debug('Extracting bundle {}'.format(path))
                        self._handle_bundle(base_path, path)
                    except Exception as e:
                        logging.exception(
                            'Unexpected exception while extracing bundle {}: {}'.format(path, str(e)))
            except Exception as e:
                logging.exception(
                    'Unexpected exception while extracing bundles in {}: {}'.format(base_path, str(e)))

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
            'path': fso.rel_path,
            'base_path': fso.base_path
        }

        if old_entry:
            sql_params = {k: '%({})s'.format(k) for k, v in vals.items()}
            q = self.db.Update(
                self.FS_TABLE, where='id = %(id)s', **sql_params)
            vals['id'] = old_entry.__id
            self.db.execute(q, vals)
            return old_entry.__id
        else:
            cols = ['parent_id', 'type', 'name', 'size', 'create_time',
                    'modify_time', 'path', 'base_path']
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
