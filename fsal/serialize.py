def str_to_bool(s):
    return str(s).lower() == "true"


def bool_to_str(b):
    return str(b).lower()


def singular_name(name):
    """Returns same string without it's trailing character, which hopefully is
    satisfactory to make the word singular, but it probably isn't."""
    return name[:-1]
