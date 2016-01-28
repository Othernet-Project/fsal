SQL = """
alter table fsentries add column base_path varchar;
"""


def up(db, conf):
    db.executescript(SQL)
