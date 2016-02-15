# -*- coding: utf-8 -*-

"""
server.py: FSAL server module

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from __future__ import print_function

from gevent import monkey
monkey.patch_all(thread=False, aggressive=True)

import os
import sys
import socket
import signal
import logging
import logging.config
from contextlib import contextmanager
from os.path import join, dirname, abspath, normpath

import gevent

import xml.etree.ElementTree as ET
from gevent.server import StreamServer

from .xmlparser import parsestring
from librarian_core.confloader import ConfDict
from .handlers import CommandHandlerFactory
from .responses import CommandResponseFactory
from .fsdbmanager import FSDBManager
from .db.databases import init_databases, close_databases


MODDIR = dirname(abspath(__file__))


def in_pkg(*paths):
    """ Return path relative to module directory """
    return normpath(join(MODDIR, *paths))


def consume_command_queue(command_queue):
    while True:
        command_handler = command_queue.get(block=True)
        command_handler.do_command()
        command_queue.task_done()


class FSALServer(object):
    IN_ENCODING = 'ascii'
    OUT_ENCODING = 'utf-8'

    def __init__(self, config, context):
        self.socket_path = config['fsal.socket']
        self.server = None
        self.handler_factory = CommandHandlerFactory(context)
        self.response_factory = CommandResponseFactory()

    def run(self):
        with self.open_socket() as sock:
            self.server = StreamServer(sock, self.request_handler)
            self.server.serve_forever()

    def stop(self):
        if self.server and self.server.started:
            self.server.stop()

    def request_handler(self, sock, address):
        try:
            request_data = parsestring(self.read_request(sock)).request
            command_data = request_data.command
            handler = self.handler_factory.create_handler(command_data)
            if handler.is_synchronous:
                self.send_response(sock, handler.do_command())
        except socket.error as e:
            logging.exception("Unable to send command response: %s" % str(e))
        except Exception as e:
            logging.exception("Unexpected exception while handling command")


    def send_response(self, sock, response_data):
        response = self.response_factory.create_response(response_data)
        response_str = response.get_xml_str(encoding=FSALServer.OUT_ENCODING)
        if not response_str[-1] == '\0':
            response_str += '\0'
        sock.sendall(response_str)

    def prepare_socket(self, path):
        try:
            os.unlink(path)
        except OSError:
            if os.path.exists(path):
                raise
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        sock.listen(1)
        return sock

    @contextmanager
    def open_socket(self):
        sock = self.prepare_socket(self.socket_path)
        try:
            yield sock
        finally:
            sock.close()

    @staticmethod
    def read_request(sock, buff_size=2048):
        data = buff = sock.recv(buff_size)
        while buff and '\0' not in buff:
            buff = sock.recv(buff_size)
            data += buff
        return data[:-1].decode(FSALServer.IN_ENCODING)

    @staticmethod
    def parse_request(request_str):
        return ET.fromstring(request_str)


def cleanup(context):
    try:
        context['fs_manager'].stop()
        context['server'].stop()
        close_databases(context['databases'])
    except Exception as e:
        logging.exception("Exception while shutting down: {}".format(str(e)))
    finally:
        logging.info('FSAL shutting down')


def configure_logging(config):
    logging.config.dictConfig({
        'version': 1,
        'root': {
            'handlers': ['file', 'console'],
            'level': config['logging.level'],
        },
        'handlers': {
            'file': {
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': config['logging.output'],
                'maxBytes': config['logging.size'],
                'backupCount': config['logging.backups'],
            },
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'level': logging.INFO,
                'stream': sys.stdout
            }
        },
        'formatters': {
            'default': {
                'format': config['logging.format'],
                'datefmt': config['logging.date_format'],
            },
        },
    })


def handle_exception(exc_type, exc_value, exc_traceback):
    logging.error("Unhandled exception",
                  exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Start FSAL server')
    parser.add_argument('--conf', metavar='PATH',
                        help='Path to configuration file',
                        default=in_pkg('fsal-server.ini'))
    args, unknown = parser.parse_known_args()

    config_path = args.conf
    config = ConfDict.from_file(config_path, catchall=True, autojson=True)

    configure_logging(config)

    context = dict()
    context['config'] = config
    context['databases'] = init_databases(config)

    fs_manager = FSDBManager(config, context)
    fs_manager.start()
    context['fs_manager'] = fs_manager

    server = FSALServer(config, context)
    context['server'] = server

    def cleanup_wrapper(*args):
        cleanup(context)

    gevent.signal(signal.SIGINT, cleanup_wrapper)
    gevent.signal(signal.SIGTERM, cleanup_wrapper)

    try:
        logging.info('FSAL server started.')
        server.run()
    except KeyboardInterrupt:
        logging.info('Keyboard interrupt received. Shutting down.')
        cleanup(context)

if __name__ == '__main__':
    main()
