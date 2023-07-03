db_table_init = """
create table if not exists operlog (
    row_id      INTEGER PRIMARY KEY,
    dttm_proc   integer,
    dttm_sent   integer,
    chat_id     integer,
    message_id  integer,
    reply_msg   integer NULL,
    text        text,
    edit_date   text NULL,
    comment     text NULL
)
"""

query_aggregated = ''
with open('sql_scripts/script_agg.sql') as sql_file:
    query_aggregated = ''.join(sql_file.readlines())