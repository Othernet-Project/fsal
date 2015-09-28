from commandtypes import COMMAND_TYPE_LIST_DIR, COMMAND_TYPE_COPY

import os
import shutil

class CommandHandler(object):
    command_type = None

    is_synchronous = True

    def __init__(self, command_data):
        self.command_data = command_data

    def do_command(self):
        result = dict()
        result['type'] = self.command_type
        return result

class DirectoryListingCommandHandler(CommandHandler):
    command_type = COMMAND_TYPE_LIST_DIR

    def do_command(self):
        path = self.command_data['params']['path']
        success = False
        dir_paths = []
        file_paths = []
        if os.path.exists(path):
            success = True
            for name in os.listdir(path):
                absolute_path = os.path.join(path, name)
                if os.path.isdir(absolute_path):
                    dir_paths.append(absolute_path)
                else:
                    file_paths.append(absolute_path)

        result = super(DirectoryListingCommandHandler, self).do_command()
        result['sucess'] = success
        result['params'] = { 'dirs':dir_paths, 'files':file_paths}
        return result

class CopyCommandHandler(CommandHandler):
    command_type = COMMAND_TYPE_COPY

    is_synchronous = False

    def do_command():
        source_path = self.command_data['params']['source']
        dest_path = self.command_data['params']['dest']
        try:
            if os.path.isdir(source_path):
                shutil.copytree(source_path, dest_path)
            else:
                shutil.copy2(source_path, dest_path)
        except:
            pass

class CommandHandlerFactory(object):
    handler_map = {
        COMMAND_TYPE_LIST_DIR:DirectoryListingCommandHandler,
        COMMAND_TYPE_COPY:CopyCommandHandler
    }

    def create_handler(self, command_data):
        command_type = command_data['type']
        if not command_type in self.handler_map:
            return None
        else:
            return self.handler_map[command_type](command_data)
