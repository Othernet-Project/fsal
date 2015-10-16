SQL = """
create table dbmgr_stats
(
    op_time real not null default 0
);

insert into dbmgr_stats(op_time) values(0);
"""


def up(db, conf):
    db.executescript(SQL)
