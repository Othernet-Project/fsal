from __future__ import absolute_import

import functools
import socket

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring

from . import commandtypes
from .fs import File, Directory
from .serialize import str_to_bool, bool_to_str


IN_ENCODING = 'utf-8'
OUT_ENCODING = 'utf-8'


def build_request_xml(command, params):
    root = Element('request')
    command_node = SubElement(root, 'command')
    type_node = SubElement(command_node, 'type')
    type_node.text = command
    params_node = SubElement(command_node, 'params')
    for key, value in params.iteritems():
        param_node = SubElement(params_node, key)
        param_node.text = value
    return root


def read_socket_stream(sock, buff_size=2048):
    data = buff = sock.recv(buff_size)
    while buff and '\0' not in buff:
        buff = sock.recv(buff_size)
        data += buff
    return data[:-1].decode(IN_ENCODING)



def command(command_type, response_parser):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            params = func(self, *args, **kwargs)
            request_xml = build_request_xml(command_type, params)
            response = self._send_request(tostring(request_xml))
            response_xml = ET.fromstring(response)
            return response_parser(self, response_xml)
        return wrapper
    return decorator


def iter_fsobjs(xml_node, constructor_func):
    for child in xml_node:
        yield constructor_func(child)


def sort_listing(fso_list):
    """
    Sort list of FSObject in-place
    """
    fso_list.sort(key=lambda fso: fso.name)

class FSAL(object):

    def __init__(self, socket_path):
        self.socket_path = socket_path

    def _send_request(self, message):
        if not message[-1] == '\0':
            message = message.encode(OUT_ENCODING) + '\0'
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.socket_path)
            sock.sendall(message)
            return read_socket_stream(sock)
        except socket.error as err:
            if sock:
                sock.close()
            sock = None
            raise RuntimeError('FSAL could not connect to FSAL server')

    def _parse_list_dir_response(self, response_xml):
        success_node = response_xml.find('.//success')
        success = str_to_bool(success_node.text)
        dirs = []
        files = []
        if success:
            base_path = response_xml.find('.//base-path').text
            dirs_node = response_xml.find('.//dirs')
            files_node = response_xml.find('.//files')
            dirs = list(iter_fsobjs(dirs_node,
                               lambda n: Directory.from_xml(base_path, n)))
            files = list(iter_fsobjs(files_node,
                                lambda n: File.from_xml(base_path, n)))
            sort_listing(dirs)
            sort_listing(files)
        return (success, dirs, files)

    def _parse_exists_response(self, response_xml):
        success_node = response_xml.find('.//success')
        success = str_to_bool(success_node.text)
        exists_node = response_xml.find('.//exists')
        exists = str_to_bool(exists_node.text)
        return success and exists

    def _parse_isdir_response(self, response_xml):
        success_node = response_xml.find('.//success')
        success = str_to_bool(success_node.text)
        isdir_node = response_xml.find('.//isdir')
        isdir = str_to_bool(isdir_node.text)
        return success and isdir

    def _parse_isfile_response(self, response_xml):
        success_node = response_xml.find('.//success')
        success = str_to_bool(success_node.text)
        isfile_node = response_xml.find('.//isfile')
        isfile = str_to_bool(isfile_node.text)
        return success and isfile

    def _parse_remove_response(self, response_xml):
        success_node = response_xml.find('.//success')
        success = str_to_bool(success_node.text)
        error_node = response_xml.find('.//error')
        error = error_node.text
        return (success, error)

    def _parse_search_response(self, response_xml):
        success, dirs, files = self._parse_list_dir_response(response_xml)
        is_match = success and str_to_bool(response_xml.find('.//is-match').text)
        return (dirs, files, is_match)

    def _parse_get_fso_response(self, response_xml):
        success_node = response_xml.find('.//success')
        success = str_to_bool(success_node.text)
        if success:
            base_path = response_xml.find('.//base-path').text
            dir_node = response_xml.find('.//dir')
            if dir_node is not None:
                return (success, Directory.from_xml(base_path, dir_node))

            file_node = response_xml.find('.//file')
            return (success, File.from_xml(base_path, file_node))

        error_node = response_xml.find('.//error')
        error = error_node.text
        return (success, error)

    @command(commandtypes.COMMAND_TYPE_LIST_DIR, _parse_list_dir_response)
    def list_dir(self, path):
        return {'path': path}

    @command(commandtypes.COMMAND_TYPE_EXISTS, _parse_exists_response)
    def exists(self, path):
        return {'path': path}

    @command(commandtypes.COMMAND_TYPE_ISDIR, _parse_isdir_response)
    def isdir(self, path):
        return {'path': path}

    @command(commandtypes.COMMAND_TYPE_ISFILE, _parse_isfile_response)
    def isfile(self, path):
        return {'path': path}

    @command(commandtypes.COMMAND_TYPE_REMOVE, _parse_remove_response)
    def remove(self, path):
        return {'path': path}

    @command(commandtypes.COMMAND_TYPE_SEARCH, _parse_search_response)
    def search(self, query, whole_words=False):
        return {'query': query,
                'whole_words': bool_to_str(whole_words)}

    @command(commandtypes.COMMAND_TYPE_GET_FSO, _parse_get_fso_response)
    def get_fso(self, path):
        return {'path': path}
