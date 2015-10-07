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


def fnwalk(path, fn, shallow=False):
    """
    Walk directory tree top-down until files or directory matching the
    predicate are found

    This generator function takes a ``path`` from which to begin the traversal,
    and a ``fn`` object that selects the paths to be returned. It calls
    ``os.listdir()`` recursively until either a full path is flagged by ``fn``
    function as valid (by returning a truthy value) or ``os.listdir()`` fails
    with ``OSError``.

    This function has been added specifically to deal with large and deep
    directory trees, and it's therefore not advisable to convert the return
    values to lists and similar memory-intensive objects.

    The ``shallow`` flag is used to terminate further recursion on match. If
    ``shallow`` is ``False``, recursion continues even after a path is matched.

    For example, given a path ``/foo/bar/bar``, and a matcher that matches
    ``bar``, with ``shallow`` flag set to ``True``, only ``/foo/bar`` is
    matched. Otherwise, both ``/foo/bar`` and ``/foo/bar/bar`` are matched.
    """
    if fn(path):
        yield path
        if shallow:
            return

    try:
        entries = scandir.scandir(path)
    except OSError:
        return

    for entry in entries:
        if entry.is_dir():
            for child in fnwalk(entry.path, fn, shallow):
                yield child
        else:
            if fn(entry.path):
                yield entry.path


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


class SearchCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_SEARCH

    def do_command(self):
        query = self.command_data['params']['query']
        if(query[0] == '/'):
            query = query[1:]
        is_match = False
        dirs = []
        files = []
        base_path = self.config['fsal.basepath']
        full_path = os.path.join(base_path, query)
        if os.path.exists(full_path):
            is_match = True
            for entry in scandir.scandir(full_path):
                rel_path = os.path.relpath(entry.path, base_path)
                if entry.is_dir():
                    dirs.append(Directory.from_path(base_path, rel_path))
                else:
                    files.append(File.from_path(base_path, rel_path))
        else:
            is_match = False
            keywords = query.split()

            def path_checker(path):
                tmp, name = os.path.split(path)
                return any(k in name for k in keywords)

            for path in fnwalk(base_path, path_checker):
                rel_path = os.path.relpath(path, base_path)
                if os.path.isdir(path):
                    dirs.append(Directory.from_path(base_path, rel_path))
                else:
                    files.append(File.from_path(base_path, rel_path))

        params = {'base_path': base_path, 'dirs': dirs, 'files': files,
                  'is_match': is_match}
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
        base_path = self.config['fsal.basepath']
        full_path = os.path.join(base_path, path)
        exists = os.path.exists(full_path)
        params = {'base_path': base_path, 'exists': exists}
        return self.send_result(success=True, params=params)


class IsDirCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_ISDIR

    def do_command(self):
        path = self.command_data['params']['path']
        base_path = self.config['fsal.basepath']
        full_path = os.path.join(base_path, path)
        isdir = os.path.isdir(full_path)
        params = {'base_path': base_path, 'isdir': isdir}
        return self.send_result(success=True, params=params)


class IsFileCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_ISFILE

    def do_command(self):
        path = self.command_data['params']['path']
        base_path = self.config['fsal.basepath']
        full_path = os.path.join(base_path, path)
        isfile = os.path.isfile(full_path)
        params = {'base_path': base_path, 'isfile': isfile}
        return self.send_result(success=True, params=params)


class RemoveCommandHandler(CommandHandler):
    command_type = commandtypes.COMMAND_TYPE_REMOVE

    def __removal(self, removal_func, path):
        base_path = self.config['fsal.basepath']
        try:
            removal_func(path)
        except Exception as exc:
            params = {'base_path': base_path, 'error': str(exc)}
            return self.send_result(success=False, params=params)
        else:
            params = {'base_path': base_path, 'error': None}
            return self.send_result(success=True, params=params)

    def do_command(self):
        path = self.command_data['params']['path']
        base_path = self.config['fsal.basepath']
        full_path = os.path.join(base_path, path)
        if not os.path.exists(full_path):
            params = {'base_path': base_path, 'error': 'does_not_exist'}
            return self.send_result(success=False, params=params)

        remover = shutil.rmtree if os.path.isdir(full_path) else os.remove
        return self.__removal(remover, full_path)


class CommandHandlerFactory(object):

    def __init__(self):
        all_handlers = CommandHandler.__subclasses__()
        self.handler_map = dict((handler_cls.command_type, handler_cls)
                                for handler_cls in all_handlers)

    def create_handler(self, command_data, config):
        command_type = command_data['type']
        try:
            handler_cls = self.handler_map[command_type]
        except KeyError:
            return None
        else:
            return handler_cls(command_data, config)
