# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_socket()

import socket
import logging
import xml.etree.ElementTree as ET

from contextlib import contextmanager

import gevent


class ONDDNotificationListener(object):

    IN_ENCODING = 'utf8'

    def __init__(self, config, callback):
        self.socket_path = config['ondd.socket']
        self.callback = callback
        self._background = None
        self.event_factory = NotificationEventFactory()

    def start(self):
        if self._background is not None:
            return
        self._background = gevent.spawn(self._process_stream)

    def stop(self):
        if self._background:
            self._background.kill()
            self._background = None

    def _process_stream(self):
        with self._connect() as sock:
            if not sock:
                logging.error('Unable to connect to ONDD for notifications')
                return

            buff_size = 2048
            buff = ''
            try:
                while True:
                    data = sock.recv(buff_size)
                    if data:
                        buff += data
                    else:
                        break
                    while True:
                        pos = buff.find('\0')
                        if pos != -1:
                            notification_str = buff[:pos].decode(self.IN_ENCODING)
                            buff = buff[pos+1:]
                            self._handle_notification_str(notification_str)
                        else:
                            break
            except socket.error as e:
                msg = 'Error while reading ONDD notification stream %s' % str(e)
                logging.exception(msg)

    def _handle_notification_str(self, notification_str):
        try:
            root = ET.fromstring(notification_str)
        except ET.ParseError:
            logging.warn('Error parsing notification %s' % notification_str)
            return

        if root.tag != 'notification':
            logging.warn('Unknown message format received %s' % notification_str)
            return
        notification = self.event_factory.create_event(root)
        if notification is not None:
            self._handle_notification(notification)

    def _handle_notification(self, notification):
        self.callback(notification)

    def _prepare_socket(self):
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.socket_path)
            return sock
        except socket.error as e:
            logging.error('Unable to connect to ONDD at %s: %s' % (
                self.socket_path, str(e)))

    @contextmanager
    def _connect(self):
        sock = self._prepare_socket()
        if sock:
            try:
                yield sock
            finally:
                if sock:
                    sock.close()
        else:
            yield None

class NotificationEvent(object):
    event = None


class FileCompleteEvent(NotificationEvent):
    event = 'file_complete'

    def __init__(self, path):
        super(FileCompleteEvent, self).__init__()
        self.path = path

    @classmethod
    def from_xml(cls, notification_xml):
        path = notification_xml.find('.//path').text
        return cls(path)


class NotificationEventFactory(object):

    def __init__(self):
        all_events = NotificationEvent.__subclasses__()
        self.events_map = dict((event_cls.event, event_cls)
                               for event_cls in all_events)

    def create_event(self, notification_xml):
        try:
            event = notification_xml.attrib['event']
            event_cls = self.events_map[event]
        except KeyError:
            return None
        else:
            return event_cls.from_xml(notification_xml)
