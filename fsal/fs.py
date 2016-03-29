import os
import logging

from datetime import datetime


class FSObject(object):

    def __init__(self, base_path, rel_path, create_date, modify_date, size):
        self._rel_path = rel_path
        self._base_path = base_path
        self._path = os.path.join(base_path, rel_path)
        self._name = os.path.split(rel_path)[1]
        self._create_date = create_date
        self._modify_date = modify_date
        self._size = size

    @property
    def path(self):
        return self._path

    @property
    def rel_path(self):
        return self._rel_path

    @property
    def base_path(self):
        return self._base_path

    @property
    def name(self):
        return self._name

    @property
    def create_date(self):
        return self._create_date

    @property
    def modify_date(self):
        return self._modify_date

    @property
    def size(self):
        return self._size

    def is_dir(self):
        return False

    def is_file(self):
        return False

    def __eq__(self, other):
        if isinstance(other, FSObject):
            return (self.path == other.path and
                    self.create_date == other.create_date and
                    self.modify_date == other.modify_date and
                    self.size == other.size and
                    self.is_file() == other.is_file())
        return NotImplemented

    def __ne__(self, other):
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def changed(self, other):
        """
        Compares two FSObject objects to determine if the content or other
        related parameters have changed. This omits checks for ctime.
        """
        if isinstance(other, FSObject):
            return (self.path != other.path or
                    self.modify_date != other.modify_date or
                    self.size != other.size)
        raise TypeError('unsupported type received: {}'.format(
            type(other).__name__))

    @classmethod
    def from_xml(cls, file_xml):
        base_path = file_xml.find('base-path').text
        rel_path = file_xml.find('rel-path').text
        size = file_xml.find('size').text
        create_timestamp = file_xml.find('create-timestamp').text
        create_date = datetime.fromtimestamp(float(create_timestamp))
        modify_timestamp = file_xml.find('modify-timestamp').text
        modify_date = datetime.fromtimestamp(float(modify_timestamp))
        return cls(base_path=base_path, rel_path=rel_path, size=size,
                   create_date=create_date, modify_date=modify_date)

    @classmethod
    def from_path(cls, base_path, rel_path):
        try:
            full_path = os.path.join(base_path, rel_path)
            stat = os.stat(full_path)
            return cls.from_stat(base_path, rel_path, stat)
        except OSError as e:
            msg = 'Error create FSObject from path %s: %s' % (rel_path, str(e))
            logging.error(msg)

    @classmethod
    def from_stat(cls, base_path, rel_path, stat):
        size = stat.st_size
        create_date = datetime.fromtimestamp(stat.st_ctime)
        modify_date = datetime.fromtimestamp(stat.st_mtime)
        return cls(base_path=base_path, rel_path=rel_path, size=size,
                   create_date=create_date, modify_date=modify_date)

    @classmethod
    def from_db_row(cls, row):
        return cls(base_path=row['base_path'], rel_path=row['path'],
                   size=row['size'], create_date=row['create_time'],
                   modify_date=row['modify_time'])


class File(FSObject):
    def is_file(self):
        return True


class Directory(FSObject):
    def is_dir(self):
        return True

    def other_path(self, path):
        path.lstrip(os.sep)
        return os.path.normpath(os.path.join(self.rel_path, path))
