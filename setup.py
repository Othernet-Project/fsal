#!/usr/bin/env python

import os
import sys
import shutil
from subprocess import check_output
from distutils.cmd import Command
from setuptools import setup, find_packages
from setuptools.command.develop import develop as DevelopCommand
from setuptools.command.sdist import sdist as SdistCommand

import fsal

SCRIPTDIR = os.path.dirname(__file__) or '.'
PY3 = sys.version_info >= (3, 0, 0)
VERSION = fsal.__version__

if '--snapshot' in sys.argv:
    sys.argv.remove('--snapshot')
    head = check_output(['git', 'rev-parse', 'HEAD'], cwd=SCRIPTDIR).strip()
    VERSION += '+git%s' % head[:8]


def read(fname):
    """ Return content of specified file """
    path = os.path.join(SCRIPTDIR, fname)
    if PY3:
        f = open(path, 'r', encoding='utf8')
    else:
        f = open(path, 'r')
    content = f.read()
    f.close()
    return content


def clean_pyc():
    print("cleaning up cached files in '%s'" % SCRIPTDIR)
    for root, dirs, files in os.walk(SCRIPTDIR):
        for f in files:
            if os.path.splitext(f)[1] == '.pyc':
                path = os.path.join(root, f)
                print("removing '%s'" % path)
                os.unlink(path)
        for d in dirs:
            if d == '__pycache__':
                path = os.path.join(root, d)
                print("removing '%s'" % path)
                shutil.rmtree(path)

class Develop(DevelopCommand):
    def run(self):
        DevelopCommand.run(self)


class Package(SdistCommand):
    def run(self):
        clean_pyc()
        SdistCommand.run(self)


class Clean(Command):
    def run(self):
        clean_pyc()

setup(
    name='fsal',
    version=VERSION,
    author='Outernet Inc',
    author_email='manish@outernet.is',
    description='Daemon for abstracting file system operations',
    license='GPLv3',
    url='https://github.com/Outernet-Project/fsal',
    packages=find_packages(),
    include_package_data=True,
    long_description=read('README.rst'),
    classifiers=[
        'Development Status :: 1 - Pre Alpha',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
    ],
    entry_points={
        'console_scripts': [
            'fsal = fsal.server:main',
            'fsal-daemon = fsal.daemon:main'
        ],
    },
    install_requires=[
        'gevent>=1.0.1',
        'python-dateutil>=2.4.2',
        'scandir>=0.9',
        'sqlize>=0.1',
        'setuptools',
        'librarian_core'
    ],
    dependency_links=[
        'git+ssh://git@github.com/Outernet-Project/librarian-core.git#egg=librarian_core-0.1',
        'git+ssh://git@github.com/Outernet-Project/librarian-content.git#egg=librarian_content-0.1',
    ],
    cmdclass={
        'develop': Develop,
        'sdist': Package,
        'uncache': Clean,
    },
)
