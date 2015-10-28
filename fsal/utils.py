
try:
    unicode = unicode
except NameError:
    unicode = str


def to_unicode(v, encoding='utf8'):
    """
    Convert a value to Unicode string (or just string in Py3). This function
    can be used to ensure string is a unicode string. This may be useful when
    input can be of different types (but meant to be used when input can be
    either bytestring or Unicode string), and desired output is always Unicode
    string.
    The ``encoding`` argument is used to specify the encoding for bytestrings.
    """
    if isinstance(v, unicode):
        return v
    try:
        return v.decode(encoding)
    except (AttributeError, UnicodeEncodeError):
        return unicode(v)


def to_bytes(v, encoding='utf8'):
    """
    Convert a value to bytestring (or just string in Py2). This function is
    useful when desired output is always a bytestring, and input can be any
    type (although it is intended to be used with strings and bytestrings).

    The ``encoding`` argument is used to specify the encoding of the resulting
    bytestring.
    """
    if isinstance(v, bytes):
        return v
    try:
        return v.encode(encoding, errors='ignore')
    except AttributeError:
        return unicode(v).encode(encoding)
