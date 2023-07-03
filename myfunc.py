from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import sqlite3 as sl
import sql_scripts

con = sl.connect('db/bot_app.db', check_same_thread=False)

def init_table():
    with con:
        con.execute(sql_scripts.db_table_init)

############ lazy data format ##########
def msg_data(message):
    return {
        'dttm_proc': str(int(datetime.now().timestamp()))
        , 'dttm_sent': str(message.date)
        , 'chat_id': str(message.chat.id)
        , 'message_id': str(message.message_id)
        , 'reply_msg': str(message.reply_to_message.message_id) if message.reply_to_message else ''
        , 'text': str(message.text)
        , 'edit_date': None
        , 'comment': None
    }
def push_to_db(message):
    msg = msg_data(message)
    q = 'INSERT INTO operlog (dttm_proc, dttm_sent, chat_id, message_id, reply_msg, text) values (%s)' % \
        f"{msg['dttm_proc']}, {msg['dttm_sent']}, {msg['chat_id']}, {msg['message_id']}, {msg['reply_msg'] or 'NULL'}, '{msg['text']}'"
    with con:
        con.execute(q)

#######################################
def generate_excel(chat_id, date_from_str):
    date_today_str = datetime.now(ZoneInfo('Europe/Moscow')).strftime('%Y-%m-%d')

    query_filtered = """
    -- сводная от даты по пользователю
    select
    	chat_id,
    	action_date,
    	location,
    	min(action_start) first_in,
    	max(action_end) last_out,
    	time(sum(duration_sec), 'unixepoch') duration
    from
    	({subq}) t
    where
    	chat_id = '%s'
    	and action_date >= '%s' -- 2023-06-30
    GROUP by chat_id, action_date, location
    """.format(subq=sql_scripts.query_aggregated.replace('%', '%%'))  ## иначе воспринимает как поле для ввода и падает

    with con:
        df = pd.DataFrame(
            con.execute(query_filtered % (chat_id, date_from_str)),
            columns=['chat_id', 'action_date', 'location', 'first_in', 'last_out', 'duration']
        )
    filepath = f"tmp/{chat_id}_{date_from_str}_{date_today_str}.xlsx"
    #df.to_excel(filepath)
    ########## auto column-width
    writer = pd.ExcelWriter(filepath, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='sheetName', index=False, na_rep='NaN')

    for column in df:
        column_length = max(df[column].astype(str).map(len).max(), len(column))
        col_idx = df.columns.get_loc(column)
        # import q; q.d()
        writer.sheets['sheetName'].set_column(col_idx, col_idx, column_length)

    writer.close() # writer.save() is deprecated
    #################

    return filepath