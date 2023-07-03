from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import logging

import telebot
import myfunc

# TODO: добавить операции /week_raw, /month_raw, /all_raw
# TODO: учитывать незакрытые события в отчете xlsx
# TODO: кнопки последних лейблов
# TODO: кнопки с флагами вкл/выкл
# TODO: исправление несоответствий по событиям входа-выхода
# TODO: редактировать редактированные -- вообще, как оно отработает?
# TODO: sql-injecions fix
# TODO: промежуточное событие - чекпоинт
# TODO: очистка папки tmp/

log_filename = "logs/pytg_inout_bot.log"

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_filename, mode='a', encoding = "UTF-8")
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s",datefmt='%Y-%m-%d %H:%M:%S')

file_handler.setFormatter(formatter)
LOGGER.addHandler(file_handler)

def infolog(msg):
	LOGGER.info(msg)
	print(datetime.now().ctime(), msg)

try:
	bot = telebot.TeleBot(os.getenv('TOKEN'))
	LOGGER.info('Bot started.')
	print('Bot started.')

	myfunc.init_table()

	@bot.message_handler(commands=['start', 'help'])
	def greet(message):
		text = """Привет\\!

Бот поможет засечь время между событиями:
	\\- входа `/in_<label>`, напр\\., /in\\_office
	\\- выхода `/out_<label>`, напр\\., /out\\_office

Если требуется поправить время конкретной метки, сделай на нее reply с текстом:
 	`-<минут>` \\-\\- отнимет указанное число минут от времени сообщения;
 	`<час>:<минут>` \\-\\- установит указанное время того же дня\\.   

Доступные команды:

/start, /help \\-\\- выведет это сообщение 

/today, /week, /month \\-\\- даст сводный \\.xlsx за последние 1, 7, 30 дней
"""
		bot.send_message(message.chat.id, text, parse_mode='MarkdownV2')

	# вывод сводной таблицы за последний день, последнюю неделю, последние 30 дней
	@bot.message_handler(commands=['today', 'week', 'month'])
	def send_excel(message):
		infolog(f"Requested aggregated data by chat_id={message.chat.id}, text='{message.text}'")
		today = datetime.now(ZoneInfo('Europe/Moscow'))

		if message.text == '/today':
			date_from = today
		elif message.text == '/week':
			date_from = today - timedelta(days=7)
		else: # month
			date_from = today - timedelta(days=30)

		filepath = myfunc.generate_excel(chat_id=message.chat.id, date_from_str=date_from.strftime('%Y-%m-%d'))
		with open(filepath, 'rb') as doc:
			bot.send_document(message.chat.id, doc)
		os.remove(filepath)

		infolog(f"File {filepath} was sent to {message.chat.id}.")

	@bot.message_handler()
	# обработка любого необработанного текстового сообщения
	def log_all(message):
		infolog('Insert attempt: ' + str(myfunc.msg_data(message)))

		myfunc.push_to_db(message)

	bot.infinity_polling()

except Exception as e:
	LOGGER.critical((type(e), e))
	print(datetime.now().ctime(), type(e), e)
	exit(1)


