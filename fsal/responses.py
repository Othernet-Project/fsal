# -*- coding: utf-8 -*-

"""
responses.py: response builders for FSAL

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from xml.etree.ElementTree import Element, SubElement, tostring

from commandtypes import COMMAND_TYPE_LIST_DIR


def create_response_xml_root():
    return Element('response')


class DirectoryListingResponse:
    def __init__(self, response_data):
        self.response_data = response_data

    def get_xml(self):
        root = create_response_xml_root()
        result_node = SubElement(root, 'result')
        success_node = SubElement(result_node, 'sucess')
        success_node.text = str(self.response_data['sucess']).lower()
        params_node = SubElement(result_node, 'params')

        dirs_paths_node = SubElement(params_node, 'dirs')
        for path in self.response_data['params']['dirs']:
            path_node = SubElement(dirs_paths_node, 'path')
            path_node.text = path

        files_paths_node = SubElement(params_node, 'files')
        for path in self.response_data['params']['files']:
            path_node = SubElement(files_paths_node, 'path')
            path_node.text = path
        return root

    def get_xml_str(self):
        return tostring(self.get_xml())


class CommandResponseFactory:
    response_map = {COMMAND_TYPE_LIST_DIR: DirectoryListingResponse}

    def create_response(self, response_data):
        command_type = response_data['type']
        if command_type not in self.response_map:
            return None
        else:
            return self.response_map[command_type](response_data)
