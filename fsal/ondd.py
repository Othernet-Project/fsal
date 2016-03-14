# -*- coding: utf-8 -*-

import logging

import gevent

from ondd_ipc.ipc import ONDDClient


class ONDDNotificationListener(object):

    IN_ENCODING = 'utf8'

    def __init__(self, config, callback):
        self.socket_path = config['ondd.socket']
        self.callback = callback
        self._background = None

    def start(self):
        if self._background is not None:
            return
        self._background = gevent.spawn(self._poll_events)

    def stop(self):
        if self._background:
            self._background.kill()
            self._background = None

    def _poll_events(self):
        ondd = ONDDClient(self.socket_path)
        while True:
            try:
                events = ondd.get_events()
                self._handle_events(events)
            except Exception as e:
                msg = 'Exception while processing events from ONDD: {}'.format(str(e))
                logging.exception(msg)
            finally:
                gevent.sleep(10)

    def _handle_events(self, events_xml):
        def filter_file_complete(e):
            try:
                return e['type'] == 'file_complete'
            except AttributeError:
                return False

        notifications = filter(filter_file_complete, events_xml)
        len_notices = len(notifications)
        if len_notices > 0:
            logging.debug('Fetched {} events from ONDD'.format(len_notices))

        if notifications:
            self._handle_notifications(notifications)

    def _handle_notifications(self, notification):
        self.callback(notification)
