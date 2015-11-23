

class FSALError(Exception):
    """Base FSAL Exception class."""
    pass


class OpenError(FSALError):
    """Raised when a file cannot be opened."""
    pass
