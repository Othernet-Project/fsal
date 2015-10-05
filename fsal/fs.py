import os

from datetime import datetime


class FSObject(object):

    def __init__(self, base_path, rel_path, create_date, modify_date):
        self._rel_path = rel_path
        self._path = os.path.join(base_path, rel_path)
        self._name = os.path.split(rel_path)[1]
        self._create_date = create_date
        self._modify_date = modify_date

    @property
    def path(self):
        return self._path

    @property
    def rel_path(self):
        return self._rel_path

    @property
    def name(self):
        return self._name

    @property
    def create_date(self):
        return self._create_date

    @property
    def modify_date(self):
        return self._modify_date


class File(FSObject):

    def __init__(self, base_path, rel_path, size, create_date, modify_date):
        super(File, self).__init__(base_path, rel_path, create_date, modify_date)
        self._size = size

    @property
    def size(self):
        return self._size

    @classmethod
    def from_xml(cls, base_path, file_xml):
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
        full_path = os.path.join(base_path, rel_path)
        size = os.path.getsize(full_path)
        create_date = datetime.fromtimestamp(os.path.getctime(full_path))
        modify_date = datetime.fromtimestamp(os.path.getmtime(full_path))
        return cls(base_path=base_path, rel_path=rel_path, size=size,
                   create_date=create_date, modify_date=modify_date)


class Directory(FSObject):

    @classmethod
    def from_xml(cls, base_path, file_xml):
        rel_path = file_xml.find('rel-path').text
        create_timestamp = file_xml.find('create-timestamp').text
        create_date = datetime.fromtimestamp(float(create_timestamp))
        modify_timestamp = file_xml.find('modify-timestamp').text
        modify_date = datetime.fromtimestamp(float(modify_timestamp))
        return cls(base_path=base_path, rel_path=rel_path, create_date=create_date,
                   modify_date=modify_date)

    @classmethod
    def from_path(cls, base_path, rel_path):
        full_path = os.path.join(base_path, rel_path)
        create_date = datetime.fromtimestamp(os.path.getctime(full_path))
        modify_date = datetime.fromtimestamp(os.path.getmtime(full_path))
        return cls(base_path=base_path, rel_path=rel_path,
                   create_date=create_date, modify_date=modify_date)
