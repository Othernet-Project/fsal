# -*- coding: utf-8 -*-

"""
responses.py: response builders for FSAL

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from datetime import datetime

from xml.etree.ElementTree import Element, SubElement, tostring

from .commandtypes import COMMAND_TYPE_LIST_DIR


def create_response_xml_root():
    return Element('response')


def to_timestamp(dt, epoch=datetime(1970, 1, 1)):
    delta = dt - epoch
    return delta.total_seconds()


class DirectoryListingResponse:
    def __init__(self, response_data):
        self.response_data = response_data

    def get_xml(self):
        root = create_response_xml_root()
        result_node = SubElement(root, 'result')
        success_node = SubElement(result_node, 'success')
        success_node.text = str(self.response_data['success']).lower()
        params_node = SubElement(result_node, 'params')

        base_path_node = SubElement(params_node, 'base-path')
        base_path_node.text = self.response_data['params']['base_path']

        dirs_node = SubElement(params_node, 'dirs')
        for d in self.response_data['params']['dirs']:
            dir_node = SubElement(dirs_node, 'dir')
            rel_path_node = SubElement(dir_node, 'rel-path')
            rel_path_node.text = d.rel_path
            create_timestamp_node = SubElement(dir_node, 'create-timestamp')
            create_timestamp_node.text = str(to_timestamp(d.create_date))
            modify_timestamp_node = SubElement(dir_node, 'modify-timestamp')
            modify_timestamp_node.text = str(to_timestamp(d.modify_date))

        files_node = SubElement(params_node, 'files')
        for f in self.response_data['params']['files']:
            file_node = SubElement(files_node, 'file')
            rel_path_node = SubElement(file_node, 'rel-path')
            rel_path_node.text = f.rel_path
            size_node = SubElement(file_node, 'size')
            size_node.text = str(f.size)
            create_timestamp_node = SubElement(file_node, 'create-timestamp')
            create_timestamp_node.text = str(to_timestamp(f.create_date))
            modify_timestamp_node = SubElement(file_node, 'modify-timestamp')
            modify_timestamp_node.text = str(to_timestamp(f.modify_date))

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
