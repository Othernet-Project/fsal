SQL = """
create table events
(
    id serial primary key not null,    -- id for event
    type varchar not null,              -- whether it's a create, modify or delete event
    src varchar not null,               -- path of the source of event
    is_dir bool not null             -- whether the source is a directory
);
"""


def up(db, conf):
    db.executescript(SQL)
