# -*- coding: utf-8 -*-

"""
handlers.py: command handlers for FSAL

Copyright 2014-2015, Outernet Inc.
Some rights reserved.

This software is free software licensed under the terms of GPLv3. See COPYING
file that comes with the source code, or http://www.gnu.org/licenses/gpl.txt.
"""

from __future__ import absolute_import

import os
import shutil

import scandir

from .import commandtypes
from .fs import File, Directory


class CommandHandler(object):
    command_type = None

    is_synchronous = True

    def __init__(self, command_data, config):
        self.command_data = command_data
        self.config = config

    def do_command(self):
        raise NotImplementedError()

    def send_result(self, **kwargs):
        result = dict(type=self.command_type)
        result.update(kwargs)
        return result


class DirectoryListingCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_LIST_DIR

    def do_command(self):
        path = self.command_data['params']['path']
        if(path[0] == '/'):
            path = path[1:]
        success = False
        dirs = []
        files = []
        base_path = self.config['fsal.basepath']
        full_path = os.path.join(base_path, path)
        if os.path.exists(full_path):
            success = True
            for entry in scandir.scandir(full_path):
                rel_path = os.path.relpath(entry.path, base_path)
                if entry.is_dir():
                    dirs.append(Directory.from_path(base_path, rel_path))
                else:
                    files.append(File.from_path(base_path, rel_path))

        params = {'base_path': base_path, 'dirs': dirs, 'files': files}
        return self.send_result(success=success, params=params)


class CopyCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_COPY

    is_synchronous = False

    def do_command(self):
        source_path = self.command_data['params']['source']
        dest_path = self.command_data['params']['dest']
        try:
            if os.path.isdir(source_path):
                shutil.copytree(source_path, dest_path)
            else:
                shutil.copy2(source_path, dest_path)
        except:
            pass


class ExistsCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_EXISTS

    def do_command(self):
        path = self.command_data['params']['path']
        base_path = self.config['fsal.basepath']
        full_path = os.path.join(base_path, path)
        exists = os.path.exists(full_path)
        params = {'base_path': base_path, 'exists': exists}
        return self.send_result(success=True, params=params)


class CommandHandlerFactory(object):
    handler_map = {
        commandtypes.COMMAND_TYPE_LIST_DIR: DirectoryListingCommandHandler,
        commandtypes.COMMAND_TYPE_COPY: CopyCommandHandler,
        commandtypes.COMMAND_TYPE_EXISTS: ExistsCommandHandler,
    }

    def create_handler(self, command_data, config):
        command_type = command_data['type']
        if command_type not in self.handler_map:
            return None
        else:
            return self.handler_map[command_type](command_data, config)
