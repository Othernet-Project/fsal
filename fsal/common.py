import os

from datetime import datetime

class File(object):

    def __init__(self, name, size, create_date, modify_date):
        self._name = name
        self._size= size
        self._create_date = create_date
        self._modify_date = modify_date

    @property
    def name(self):
        return self._name

    @property
    def size(self):
        return self._size

    @property
    def create_date(self):
        return self._create_date

    @property
    def modify_date(self):
        return self._modify_date

    @classmethod
    def from_xml(cls, file_xml):
        name = file_xml.find('name').text
        size = file_xml.find('size').text
        create_timestamp = file_xml.find('create-timestamp').text
        create_date = datetime.fromtimestamp(float(create_timestamp))
        modify_timestamp = file_xml.find('modify-timestamp').text
        modify_date = datetime.fromtimestamp(float(modify_timestamp))
        return cls(name=name, size=size, create_date=create_date,
                   modify_date=modify_date)

    @classmethod
    def from_path(cls, file_path):
        head, name = os.path.split(file_path)
        size = os.path.getsize(file_path)
        create_date = datetime.fromtimestamp(os.path.getctime(file_path))
        modify_date = datetime.fromtimestamp(os.path.getmtime(file_path))
        return cls(name=name, size=size, create_date=create_date,
                   modify_date=modify_date)


class Directory(object):

    def __init__(self, name, create_date, modify_date):
        self._name = name
        self._create_date = create_date
        self._modify_date = modify_date

    @property
    def name(self):
        return self._name

    @property
    def create_date(self):
        return self._create_date

    @property
    def modify_date(self):
        return self._modify_date

    @classmethod
    def from_xml(cls, file_xml):
        name = file_xml.find('name').text
        create_timestamp = file_xml.find('create-timestamp').text
        create_date = datetime.fromtimestamp(float(create_timestamp))
        modify_timestamp = file_xml.find('modify-timestamp').text
        modify_date = datetime.fromtimestamp(float(modify_timestamp))
        return cls(name=name, create_date=create_date,
                   modify_date=modify_date)

    @classmethod
    def from_path(cls, file_path):
        head, name = os.path.split(file_path)
        create_date = datetime.fromtimestamp(os.path.getctime(file_path))
        modify_date = datetime.fromtimestamp(os.path.getmtime(file_path))
        return cls(name=name, create_date=create_date,
                   modify_date=modify_date)


