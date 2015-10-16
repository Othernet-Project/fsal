import sys

import scandir


PY2 = sys.version_info.major == 2
PY3 = sys.version_info.major == 3


if PY3:
    basestring = str
    unicode = str
if PY2:
    unicode = unicode
    basestring = basestring


def fnwalk(path, fn, shallow=False):
    """
    Walk directory tree top-down and "visit" files or directory matching the
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


def validate_path(base_path, path):
    path = path.lstrip(os.sep)
    full_path = os.path.abspath(os.path.join(base_path, path))
    return full_path.startswith(base_path), full_path
