"""
events.py: Events denoting changes to the filesystem

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

import logging

from .serialize import str_to_bool


EVENT_CREATED = 'created'
EVENT_DELETED = 'deleted'
EVENT_MODIFIED = 'modified'


class FileSystemEvent(object):
    """Base event type for file system events"""

    event_type = None

    is_dir = False

    def __init__(self, src):
        self._src = src

    @property
    def src(self):
        """Source path which generated this event"""
        return self._src


class FileCreatedEvent(FileSystemEvent):
    """Represents file creation event"""

    event_type = EVENT_CREATED


class FileDeletedEvent(FileSystemEvent):
    """Represents file deletion event"""

    event_type = EVENT_DELETED


class FileModifiedEvent(FileSystemEvent):
    """Represents file modification event"""

    event_type = EVENT_MODIFIED


class DirCreatedEvent(FileSystemEvent):
    """Represents dir creation event"""

    event_type = EVENT_CREATED

    is_dir = True


class DirDeletedEvent(FileSystemEvent):
    """Represents dir deletion event"""

    event_type = EVENT_DELETED

    is_dir = True


class DirModifiedEvent(FileSystemEvent):
    """Represents dir modification event"""

    event_type = EVENT_MODIFIED

    is_dir = True


EVENTS_MAP = {
    (EVENT_CREATED, False): FileCreatedEvent,
    (EVENT_DELETED, False): FileDeletedEvent,
    (EVENT_MODIFIED, False): FileModifiedEvent,
    (EVENT_CREATED, True): DirCreatedEvent,
    (EVENT_DELETED, True): DirDeletedEvent,
    (EVENT_MODIFIED, True): DirModifiedEvent,
}


def event_from_xml(node):
    type = node.find('type').text
    src =  node.find('src').text
    is_dir = str_to_bool(node.find('is_dir').text)
    key = (type, is_dir)
    event_cls = EVENTS_MAP[key]
    if event_cls:
        return event_cls(src)


def event_from_row(row):
    key = (row.type, row.is_dir)
    cls = EVENTS_MAP[key]
    if cls:
        return cls(row.src)


def get_event_dict(event):
    return {
        "type": event.event_type,
        "src": event.src,
        "is_dir":event.is_dir
    }


class FileSystemEventQueue(object):

    EVENTS_TABLE = 'events'

    def __init__(self, config, context):
        self.db = context['databases'].fs

    def add(self, event):
        cols = ['type', 'src', 'is_dir']
        vals = get_event_dict(event)
        q = self.db.Insert(self.EVENTS_TABLE, cols=cols)
        self.db.execute(q, vals)

    def additems(self, events):
        cols = ['type', 'src', 'is_dir']
        q = self.db.Insert(self.EVENTS_TABLE, cols=cols)
        vals = (get_event_dict(e) for e in events)
        self.db.executemany(q, vals)

    def getitems(self, maxnum=100):
        items = []
        with self.db.transaction():
            q = self.db.Select(what='*', sets=self.EVENTS_TABLE, limit=maxnum,
                               order='id')
            row_iter = self.db.fetchiter(q)
            for row in row_iter:
                items.append(event_from_row(row))
        return items

    def delitems(self, num):
        with self.db.transaction():
            ids = []
            q = self.db.Select(what='id', sets=self.EVENTS_TABLE, limit=num,
                               order='id')
            row_iter = self.db.fetchiter(q)
            for row in row_iter:
                ids.append(row.id)
            q = self.db.Delete(self.EVENTS_TABLE, where='id = %s')
            self.db.executemany(q, ((id,) for id in ids))
            logging.debug('Cleared %d events' % num)
