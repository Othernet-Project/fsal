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

from .import commandtypes


class CommandHandler(object):
    command_type = None

    is_synchronous = True

    def __init__(self, context, command_data):
        self.command_data = command_data
        self.fs_mgr = context['fs_manager']

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
        success, fs_objs = self.fs_mgr.list_dir(path)
        dirs = []
        files = []
        for fso in fs_objs:
            if fso.is_dir():
                dirs.append(fso)
            else:
                files.append(fso)
        params = {'base_path': self.fs_mgr.base_path, 'dirs': dirs,
                  'files': files}
        return self.send_result(success=success, params=params)


class SearchCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_SEARCH

    def do_command(self):
        query = self.command_data['params']['query']
        is_match, fs_objs = self.fs_mgr.search(query)
        dirs = []
        files = []
        for fso in fs_objs:
            if fso.is_dir():
                dirs.append(fso)
            else:
                files.append(fso)
        params = {'base_path': self.fs_mgr.base_path, 'dirs': dirs,
                  'files': files, 'is_match': is_match}
        return self.send_result(success=True, params=params)


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
        if path is None:
            exists = False
        else:
            exists = self.fs_mgr.exists(path)
        params = {'exists': exists}
        return self.send_result(success=True, params=params)


class IsDirCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_ISDIR

    def do_command(self):
        path = self.command_data['params']['path']
        if path is None:
            isdir = False
        else:
            isdir = self.fs_mgr.is_dir(path)
        params = {'isdir': isdir}
        return self.send_result(success=True, params=params)


class IsFileCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_ISFILE

    def do_command(self):
        path = self.command_data['params']['path']
        if path is None:
            isfile = False
        else:
            isfile = self.fs_mgr.is_file(path)
        params = {'isfile': isfile}
        return self.send_result(success=True, params=params)


class RemoveCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_REMOVE

    def do_command(self):
        path = self.command_data['params']['path']
        success, msg = self.fs_mgr.remove(path)
        params = {'error': msg}
        return self.send_result(success=success, params=params)


class CommandHandlerFactory(object):

    def __init__(self, context):
        self.context = context
        all_handlers = CommandHandler.__subclasses__()
        self.handler_map = dict((handler_cls.command_type, handler_cls)
                                for handler_cls in all_handlers)

    def create_handler(self, command_data):
        command_type = command_data['type']
        try:
            handler_cls = self.handler_map[command_type]
        except KeyError:
            return None
        else:
            return handler_cls(self.context, command_data)
