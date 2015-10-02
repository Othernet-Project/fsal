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
from contextlib import contextmanager

import xmltodict
import xml.etree.ElementTree as ET
from gevent.server import StreamServer

from handlers import CommandHandlerFactory
from responses import CommandResponseFactory

IN_ENCODING = 'ascii'
OUT_ENCODING = 'utf-8'

SOCKET_PATH = './fsal_socket'

handler_factory = CommandHandlerFactory()
response_factory = CommandResponseFactory()


def consume_command_queue(command_queue):
    while True:
        command_handler = command_queue.get(block=True)
        command_handler.do_command()
        command_queue.task_done()


def request_handler(sock, address):
    request_data = xmltodict.parse(read_request(sock))['request']
    command_data = request_data['command']
    handler = handler_factory.create_handler(command_data)
    if handler.is_synchronous:
        send_response(sock, handler.do_command())
    sock.close()


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


def prepare_socket(path):
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
def open_socket():
    sock = prepare_socket(SOCKET_PATH)
    try:
        yield sock
    finally:
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()


def main():
    with open_socket() as sock:
        server = StreamServer(sock, request_handler)
        server.serve_forever()


if __name__ == '__main__':
    main()
