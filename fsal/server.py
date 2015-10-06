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
import socket
import signal
from contextlib import contextmanager
from os.path import join, dirname, abspath, normpath

import xmltodict
import xml.etree.ElementTree as ET
from gevent.server import StreamServer

from .confloader import ConfDict
from .handlers import CommandHandlerFactory
from .responses import CommandResponseFactory

IN_ENCODING = 'ascii'
OUT_ENCODING = 'utf-8'

MODDIR = dirname(abspath(__file__))

handler_factory = CommandHandlerFactory()
response_factory = CommandResponseFactory()


def in_pkg(*paths):
    """ Return path relative to module directory """
    return normpath(join(MODDIR, *paths))


def consume_command_queue(command_queue):
    while True:
        command_handler = command_queue.get(block=True)
        command_handler.do_command()
        command_queue.task_done()


def read_request(sock, buff_size=2048):
    data = buff = sock.recv(buff_size)
    while buff and '\0' not in buff:
        buff = sock.recv(buff_size)
        data += buffer
    return data[:-1].decode(IN_ENCODING)


def send_response(sock, response_data):
    response = response_factory.create_response(response_data)
    response_str = response.get_xml_str().encode(OUT_ENCODING)
    if not response_str[-1] == '\0':
        response_str += '\0'
    sock.sendall(response_str)


def parse_request(request_str):
    return ET.fromstring(request_str)


class FSALServer(object):

    def __init__(self, config):
        self._config = config
        self._server = None

    def run(self):
        with self.open_socket() as sock:
            self._server = StreamServer(sock, self._request_handler)
            self._server.serve_forever()

    def stop(self):
        if self._server and self._server.started:
            self._server.stop()

    def _request_handler(self, sock, address):
        request_data = xmltodict.parse(read_request(sock))['request']
        command_data = request_data['command']
        handler = handler_factory.create_handler(command_data, self._config)
        if handler.is_synchronous:
            send_response(sock, handler.do_command())

    def prepare_socket(self, path):
        try:
            os.unlink(path)
        except OSError:
            if(os.path.exists(path)):
                raise
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(path)
        sock.listen(1)
        return sock

    @contextmanager
    def open_socket(self):
        sock = self.prepare_socket(self._config['fsal.socket'])
        try:
            yield sock
        finally:
            sock.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Start FSAL server')
    parser.add_argument('--conf', metavar='PATH',
                        help='Path to configuration file',
                        default=in_pkg('fsal-server.ini'))
    args = parser.parse_args()

    config_path = args.conf
    config = ConfDict.from_file(config_path, catchall=True, autojson=True)

    server = FSALServer(config)

    signal.signal(signal.SIGINT, lambda *a, **k: server.stop())
    signal.signal(signal.SIGTERM, lambda *a, **k: server.stop())

    try:
        server.run()
    except KeyboardInterrupt:
        print('Keyboard interrupt received')
    server.stop()

if __name__ == '__main__':
    main()
