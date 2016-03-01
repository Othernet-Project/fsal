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
from .serialize import str_to_bool


def validate_path(base_path, path):
    path = path.lstrip(os.sep)
    full_path = os.path.abspath(os.path.join(base_path, path))
    return full_path.startswith(base_path), full_path


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
        path = self.command_data.params.path.data
        success, fs_objs = self.fs_mgr.list_dir(path)
        dirs = []
        files = []
        for fso in fs_objs:
            if fso.is_dir():
                dirs.append(fso)
            else:
                files.append(fso)
        params = {'dirs': dirs, 'files': files}

        return self.send_result(success=success, params=params)


class SearchCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_SEARCH

    def do_command(self):
        params = self.command_data.params
        query = params.query.data
        whole_words = str_to_bool(params.whole_words.data)
        if len(params.excludes.children) > 0:
            exclude = []
            for c in params.excludes.children:
                exclude.append(c.data)
        else:
            exclude = None
        is_match, fs_objs = self.fs_mgr.search(query, whole_words=whole_words,
                                               exclude=exclude)
        dirs = []
        files = []
        for fso in fs_objs:
            if fso.is_dir():
                dirs.append(fso)
            else:
                files.append(fso)
        params = {'dirs': dirs, 'files': files, 'is_match': is_match}

        return self.send_result(success=True, params=params)


class ListBasePathsCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_LIST_BASE_PATHS

    def do_command(self):
        return self.send_result(success=True, params={'paths':
                                                       self.fs_mgr.base_paths})


class GetPathSizeCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_GET_PATH_SIZE

    def do_command(self):
        path = self.command_data.params.path.data
        resp = self.fs_mgr.get_path_size(path)
        return self.send_result(success=resp)


class ConsolidateCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_CONSOLIDATE

    def do_command(self):
        source_paths = [s.data for s in self.command_data.params.sources.children]
        dest_path = self.command_data.params.dest.data
        try:
            resp = self.fs_mgr.consolidate(source_paths, dest_path)
        except AssertionError:
            # this should create a dismissable notification with details about
            # the error
            resp = False
        return self.send_result(success=resp)


class CopyCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_COPY

    is_synchronous = False

    def do_command(self):
        source_path = self.command_data.params.source.data
        dest_path = self.command_data.params.dest.data
        source_valid, source_path = validate_path(self.base_path, source_path)
        dest_valid, dest_path = validate_path(self.base_path, dest_path)
        if not (source_valid and dest_valid):
            return
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
        path = self.command_data.params.path.data
        unindexed = str_to_bool(self.command_data.params.unindexed.data)

        if path is None:
            exists = False
        else:
            exists = self.fs_mgr.exists(path, unindexed)
        params = {'exists': exists}
        return self.send_result(success=True, params=params)


class IsDirCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_ISDIR

    def do_command(self):
        path = self.command_data.params.path.data
        if path is None:
            isdir = False
        else:
            isdir = self.fs_mgr.is_dir(path)
        params = {'isdir': isdir}
        return self.send_result(success=True, params=params)


class IsFileCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_ISFILE

    def do_command(self):
        path = self.command_data.params.path.data
        if path is None:
            isfile = False
        else:
            isfile = self.fs_mgr.is_file(path)
        params = {'isfile': isfile}
        return self.send_result(success=True, params=params)


class RemoveCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_REMOVE

    def do_command(self):
        path = self.command_data.params.path.data
        success, msg = self.fs_mgr.remove(path)
        params = {'error': msg}
        return self.send_result(success=success, params=params)


class GetFSOCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_GET_FSO

    def do_command(self):
        path = self.command_data.params.path.data
        fso = self.fs_mgr.get_fso(path)
        success = fso is not None
        if success:
            params = dict()
            key = 'dir' if fso.is_dir() else 'file'
            params[key] = fso
        else:
            params = {'error': 'does_not_exist'}

        return self.send_result(success=success, params=params)


class TransferCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_TRANSFER

    def do_command(self):
        src = self.command_data.params.src.data
        dest = self.command_data.params.dest.data
        success, msg = self.fs_mgr.transfer(src, dest)
        params = {'error': msg}
        return self.send_result(success=success, params=params)


class GetChangesCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_GET_CHANGES

    def do_command(self):
        limit = int(self.command_data.params.limit.data)
        events = self.fs_mgr.get_changes(limit)
        params = {'events': events}
        return self.send_result(success=True, params=params)


class ConfirmChangesCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_CONFIRM_CHANGES

    def do_command(self):
        limit = int(self.command_data.params.limit.data)
        self.fs_mgr.confirm_changes(limit)
        return self.send_result(success=True, params={})


class RefreshPathCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_REFRESH_PATH

    def do_command(self):
        path = self.command_data.params.path.data
        success, msg = self.fs_mgr.refresh_path(path)
        params = {'error': msg}
        return self.send_result(success=success, params=params)


class RefreshCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_REFRESH

    def do_command(self):
        self.fs_mgr.refresh()
        return self.send_result(success=True, params={})


class CommandHandlerFactory(object):

    def __init__(self, context):
        self.context = context
        all_handlers = CommandHandler.__subclasses__()
        self.handler_map = dict((handler_cls.command_type, handler_cls)
                                for handler_cls in all_handlers)

    def create_handler(self, command_data):
        command_type = command_data.type.data
        try:
            handler_cls = self.handler_map[command_type]
        except KeyError:
            return None
        else:
            return handler_cls(self.context, command_data)
