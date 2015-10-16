SQL = """
create table fsentries
(
    id integer primary key not null,              -- id for the file
    parent_id integer not null default 0,         -- id of the parent
    type integer not null,                        -- determines the type of the entry (0 => file, 1 => directory)
    name varchar not null,                        -- filename
    size integer not null default 0,              -- size in bytes
    create_time timestamp not null,               -- UNIX timestamp of created time
    modify_time timestamp not null,               -- UNIX timestamp of modified time
    path varchar unique not null                         -- path relative to base path
);
"""


def up(db, conf):
    db.executescript(SQL)
