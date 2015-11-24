SQL = """
create index path_index on fsentries(path);
"""


def up(db, conf):
    db.executescript(SQL)
