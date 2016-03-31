"""
gevent based async file I/O utility functions
Inspired from stdlib shutil
"""

from __future__ import unicode_literals

import os
import sys
import stat
import errno

from shutil import copystat, _samefile, _basename, _destinsrc
from shutil import Error, WindowsError, SpecialFileError

import gevent

Error = Error
basename = _basename

SLEEP_INTERVAL = 0.001


def safe_copystat(src, dst):
    try:
        copystat(src, dst)
    except OSError as exc:
        # is it copying to file systems that do not support this operation?
        if exc.errno != errno.EPERM:
            raise


def copyfileobj(fsrc, fdst, buff_size=16*1024):
    """copy data from file-like object fsrc to file-like object fdst"""
    while True:
        buf = fsrc.read(buff_size)
        if not buf:
            break
        fdst.write(buf)
        gevent.sleep(SLEEP_INTERVAL)


def copy(src, dst):
    """Copy data and mode bits ("cp src dst").

    The destination may be a directory.

    """
    if os.path.isdir(dst):
        dst = os.path.join(dst, os.path.basename(src))
    copyfile(src, dst)
    safe_copystat(src, dst)


def copyfile(src, dst):
    """Copy data from src to dst"""
    if _samefile(src, dst):
        raise Error("`%s` and `%s` are the same file" % (src, dst))

    for fn in [src, dst]:
        try:
            st = os.stat(fn)
        except OSError:
            # File most likely does not exist
            pass
        else:
            # XXX What about other special files? (sockets, devices...)
            if stat.S_ISFIFO(st.st_mode):
                raise SpecialFileError("`%s` is a named pipe" % fn)

    with open(src, 'rb') as fsrc:
        with open(dst, 'wb') as fdst:
            copyfileobj(fsrc, fdst)


def copytree(src, dst, symlinks=False, ignore=None, merge=False, copied=None):
    """Recursively copy a directory tree using copy2().

    The destination directory must not already exist.
    If exception(s) occur, an Error is raised with a list of reasons.

    If the optional symlinks flag is true, symbolic links in the
    source tree result in symbolic links in the destination tree; if
    it is false, the contents of the files pointed to by symbolic
    links are copied.

    The optional ignore argument is a callable. If given, it
    is called with the `src` parameter, which is the directory
    being visited by copytree(), and `names` which is the list of
    `src` contents, as returned by os.listdir():

        callable(src, names) -> ignored_names

    Since copytree() is called recursively, the callable will be
    called once for each directory that is copied. It returns a
    list of names relative to the `src` directory that should
    not be copied.

    XXX Consider this example code rather than the ultimate tool.

    """
    names = os.listdir(src)
    if ignore is not None:
        ignored_names = ignore(src, names)
    else:
        ignored_names = set()

    try:
        os.makedirs(dst)
    except os.error as e:
        if not merge:
            raise Error(e)
    errors = []
    for name in names:
        if name in ignored_names:
            continue
        srcname = os.path.join(src, name)
        dstname = os.path.join(dst, name)
        try:
            if symlinks and os.path.islink(srcname):
                linkto = os.readlink(srcname)
                os.symlink(linkto, dstname)
            elif os.path.isdir(srcname):
                copytree(srcname, dstname, symlinks, ignore, merge, copied)
                gevent.sleep(SLEEP_INTERVAL)
            else:
                # Will raise a SpecialFileError for unsupported file types
                copy(srcname, dstname)
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except Error as err:
            errors.extend(err.args[0])
        except EnvironmentError as why:
            errors.append((srcname, dstname, str(why)))
        else:
            if copied is not None:
                copied.append(srcname)
    try:
        safe_copystat(src, dst)
    except OSError as why:
        if WindowsError is not None and isinstance(why, WindowsError):
            # Copying file access times may fail on Windows
            pass
        else:
            errors.append((src, dst, str(why)))
    else:
        if copied is not None:
            copied.append(src)

    if errors:
        raise Error(errors)


def rm(path, **kwargs):
    """
    Helper function to invoke correct remover function for files and dirs
    """
    if os.path.isdir(path):
        rmtree(path)
    else:
        os.remove(path)


def rmtree(path, ignore_errors=False, onerror=None):
    """Recursively delete a directory tree.

    If ignore_errors is set, errors are ignored; otherwise, if onerror
    is set, it is called to handle the error with arguments (func,
    path, exc_info) where func is os.listdir, os.remove, or os.rmdir;
    path is the argument to that function that caused it to fail; and
    exc_info is a tuple returned by sys.exc_info().  If ignore_errors
    is false and onerror is None, an exception is raised.

    """
    if ignore_errors:
        def onerror(*args):
            pass
    elif onerror is None:
        def onerror(*args):
            raise
    try:
        if os.path.islink(path):
            # symlinks to directories are forbidden, see bug #1669
            raise OSError("Cannot call rmtree on a symbolic link")
    except OSError:
        onerror(os.path.islink, path, sys.exc_info())
        # can't continue even if onerror hook returns
        return
    names = []
    try:
        names = os.listdir(path)
    except os.error:
        onerror(os.listdir, path, sys.exc_info())
    for name in names:
        fullname = os.path.join(path, name)
        try:
            mode = os.lstat(fullname).st_mode
        except os.error:
            mode = 0
        if stat.S_ISDIR(mode):
            rmtree(fullname, ignore_errors, onerror)
            gevent.sleep(SLEEP_INTERVAL)
        else:
            try:
                os.remove(fullname)
                gevent.sleep(SLEEP_INTERVAL)
            except os.error:
                onerror(os.remove, fullname, sys.exc_info())
    try:
        os.rmdir(path)
    except os.error:
        onerror(os.rmdir, path, sys.exc_info())


def move(src, dst):
    """Recursively move a file or directory to another location. This is
    similar to the Unix "mv" command.

    If the destination is a directory or a symlink to a directory, the source
    is moved inside the directory. The destination path must not already
    exist.

    If the destination already exists but is not a directory, it may be
    overwritten depending on os.rename() semantics.

    If the destination is on our current filesystem, then rename() is used.
    Otherwise, src is copied to the destination and then removed.
    A lot more could be done here...  A look at a mv.c shows a lot of
    the issues this implementation glosses over.

    """
    real_dst = dst
    if os.path.isdir(dst):
        if _samefile(src, dst):
            # We might be on a case insensitive filesystem,
            # perform the rename anyway.
            os.rename(src, dst)
            return

        real_dst = os.path.join(dst, _basename(src))
        if os.path.exists(real_dst):
            raise Error("Destination path '%s' already exists" % real_dst)
    try:
        os.rename(src, real_dst)
    except OSError:
        if os.path.isdir(src):
            if _destinsrc(src, dst):
                msg = "Cannot move a directory '%s' into itself '%s'." % (src,
                                                                          dst)
                raise Error(msg)
            copytree(src, real_dst, symlinks=True)
            rmtree(src)
        else:
            copy(src, real_dst)
            os.unlink(src)
