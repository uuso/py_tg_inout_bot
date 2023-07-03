/* Преобразование изначального набора записей в столбцах 
	 * row_id, chat_id, dttm_sent, message_id, reply_msg, text
 * В следующий:
	 * chat_id, 
	 * action_date, action_start, action_end, 
	 * location, action_name, 
	 * duration_sec, duration_days, duration_time
 */
with subq1 as (
	-- коррекция времени, извлечение префиксов и локейшн
	select
		left_part.row_id,
		left_part.chat_id,
		case 
			when instr(right_part.text,':') > 0 
			then left_part.dttm_sent - (left_part.dttm_sent % (3600*24)) + unixepoch(right_part.text) - unixepoch('00:00:00') - 3600*3
			else left_part.dttm_sent + 60*coalesce(cast(right_part.text as integer), 0)
		end dttm_int,
		left_part.text message,
		case
			when instr(left_part.text, '/') = 1 and instr(left_part.text,'_') > 2 -- начинается с '/' и есть хотя бы один символ перед '_'
			then substr(left_part.text, 2, instr(left_part.text, '_')-2)
			else NULL
		end prefix,
		case
			when instr(left_part.text, '/') = 1 and instr(left_part.text,'_') > 2 -- начинается с '/' и есть префикс
				then substr(
					left_part.text, 
					instr(left_part.text, '_') + 1, 
					LENGTH(left_part.text) - instr(left_part.text, '_')
				)
			when instr(left_part.text, '/') = 1 -- начинается с '/' и без префикса
				then substr(left_part.text, 2)
			else NULL
		end location
	from
		operlog left_part
	left join operlog right_part
		on
			left_part.message_id = right_part.reply_msg
	WHERE
		left_part.reply_msg is NULL -- только правки могут быть reply
),
subq2 as (
	-- считая, что префиксы парные и корректны, создаем группы
	-- также, читаемый dttm
	-- для дальнейшего процесса вычитания, устанавливаем принудительный порядок префиксов (Меньше - раньше)	
	/*
	можно добавить проверку на количество in и out
	а когда джойню - следить что out > in иначе NULL
	если не хватает out: из двух предшествующих инов оставить ближайший
	*/
	select
		row_id,
		chat_id,
		dttm_int,
		DATETIME(dttm_int, 'unixepoch', 'localtime') dttm_txt,
		message,
		location,
		prefix,
		case prefix
			when 'in' then 1
			when 'out' then 2
			else NULL
		end prefix_order,
		row_number() over (
			partition by 
				chat_id, location, prefix 
			order by 
				dttm_int
		) prefix_group_id
	from subq1
	order by dttm_int
),
subq3 as (
	-- вычисление секунд и вида события, например, in_out
	-- out_in не создаются -- NULL из-за LEAD по партиции
	/*
	 * тут должна быть проверка, что внутри группы по chat_id, location, prefix_group_id
	 * порядок prefix_order соответствует хронологическому
	 * это будет означать, что события не пропущены
	 */
	select
		*,
		lead(dttm_int) 					over(partition by chat_id, location, prefix_group_id order by dttm_int) - dttm_int 	sec_to_next,
		prefix || '_' || lead(prefix) 	over(partition by chat_id, location, prefix_group_id order by dttm_int) 			pref_action,
		lead(dttm_txt) 					over(partition by chat_id, location, prefix_group_id order by dttm_int) 			next_dttm_txt
	from subq2
)
select
	row_number() over(order by dttm_txt) record_id,
	chat_id,
	date(dttm_int + 3600*3, 'unixepoch') action_date,
	dttm_txt action_start,
	next_dttm_txt action_end,
	location,
	pref_action action_name,
	sec_to_next duration_sec,
	sec_to_next / (3600*24) duration_days,
	time(sec_to_next, 'unixepoch') duration_time
from
	subq3
where pref_action is not NULL
order by chat_id, dttm_int
