SQL = """
alter table fsentries alter column size type bigint;
"""


def up(db, conf):
    db.executescript(SQL)
