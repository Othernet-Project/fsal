# -*- coding: utf-8 -*-

import socket
import logging
import xml.etree.ElementTree as ET

from contextlib import contextmanager
from itertools import ifilter

import gevent

from ondd_ipc.ipc import xml_get_path, IN_ENCODING, OUT_ENCODING,\
    ONDD_SOCKET_TIMEOUT


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
        self._background = gevent.spawn(self._poll_events)

    def stop(self):
        if self._background:
            self._background.kill()
            self._background = None

    def _poll_events(self):
        while True:
            try:
                events_xml = self._query_events()
                if events_xml is not None:
                    self._handle_events(events_xml)
            except Exception as e:
                msg = 'Exception while processing events from ONDD: {}'.format(str(e))
                logging.exception(msg)
            finally:
                gevent.sleep(10)

    def _query_events(self):
        query = xml_get_path('/events')
        root = self._send(query)
        if root is not None:
            return root.find('events')
        else:
            return None

    def _handle_events(self, events_xml):
        def filter_file_complete(e):
            try:
                return e.find('type').text == 'file_complete'
            except AttributeError:
                return False

        file_complete_events = ifilter(filter_file_complete, events_xml)
        notifications = [self.event_factory.create_event(e)
                         for e in file_complete_events]
        logging.debug('Fetched {} events from ONDD'.format(len(notifications)))

        if notifications:
            self._handle_notifications(notifications)

    def _send(self, payload):
        if not payload[-1] == '\0':
            payload = payload.encode(OUT_ENCODING) + '\0'
        try:
            with self._connect() as sock:
                sock.send(payload)
                data = self._read(sock)
        except (socket.error, socket.timeout):
            return None
        else:
            return self._parse(data)

    def _read(self, sock, buffsize=2048):
        idata = data = sock.recv(buffsize)
        while idata and b'\0' not in idata:
            idata = sock.recv(buffsize)
            data += idata
        return data[:-1].decode(IN_ENCODING)

    def _parse(self, data):
        return ET.fromstring(data.encode('utf8'))

    def _handle_notifications(self, notification):
        self.callback(notification)

    def _prepare_socket(self):
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(ONDD_SOCKET_TIMEOUT)
            sock.connect(self.socket_path)
            return sock
        except socket.error as e:
            logging.error('Unable to connect to ONDD at %s: %s.' % (
                self.socket_path, str(e)))
            raise

    @contextmanager
    def _connect(self):
        sock = self._prepare_socket()
        try:
            yield sock
        finally:
            try:
                sock.shutdown(socket.SHUT_RDWR)
                sock.close()
            except:
                pass


class NotificationEvent(object):
    event = None


class FileCompleteEvent(NotificationEvent):
    event = 'file_complete'

    def __init__(self, path):
        super(FileCompleteEvent, self).__init__()
        self.path = path

    @classmethod
    def from_xml(cls, event_xml):
        try:
            path = event_xml.find('.//path').text
            return cls(path)
        except AttributeError:
            return None


class NotificationEventFactory(object):

    def __init__(self):
        all_events = NotificationEvent.__subclasses__()
        self.events_map = dict((event_cls.event, event_cls)
                               for event_cls in all_events)

    def create_event(self, event_xml):
        try:
            event_type = event_xml.find('type').text
            event_cls = self.events_map[event_type]
        except AttributeError:
            return None
        else:
            return event_cls.from_xml(event_xml)
