import os
import time
from time import sleep
from slack import WebClient
from slack.errors import SlackApiError
import psycopg2
import logging.handlers
import logging
import traceback
log_handlers = [logging.handlers.RotatingFileHandler(filename='/home/coinbot/update_trade_target_log.txt', maxBytes=1024*10), logging.StreamHandler()]
logging.basicConfig(level = logging.INFO, format = '%(asctime)s [%(levelname)s] : %(message)s', handlers = log_handlers)
logger = logging.getLogger('trade_logger')

start = time.time()
getting_interval = 15
alarm_threshold = 0.5#상승 또는 하락 평균
max_reset_auto_trade_count = 30
delete_auto_trade_count = 0
loop_count = 0
plus_price_delta_select_query = f"""
select  delta_total.ticker_name ticker_name
	, coalesce(total_avg_delta,0) total_avg_delta, coalesce(total_cnt_delta,0) total_cnt_delta, coalesce(plus_avg_delta,0) plus_avg_delta
	, coalesce(plus_cnt_delta,0) plus_cnt_delta, coalesce(minus_avg_delta,0) minus_avg_delta, coalesce(minus_cnt_delta,0) minus_cnt_delta
    ,get_code('market',delta_total.ticker_name) korean_name
from
(
select ticker_name, round(cast(avg(delta) as numeric),1) total_avg_delta, count(delta) total_cnt_delta
from public.ticker_price_delta_fact
where sequence_number >= (SELECT last_value-{getting_interval} FROM sequence_main)
group by ticker_name
) delta_total
left join (
select ticker_name, round(cast(avg(delta) as numeric),1) plus_avg_delta, count(delta) plus_cnt_delta
from public.ticker_price_delta_fact
where sequence_number >= (SELECT last_value-{getting_interval} FROM sequence_main)
      and delta > 0
group by ticker_name
) delta_plus
on delta_total.ticker_name = delta_plus.ticker_name
left join (
select ticker_name, round(cast(avg(delta) as numeric),1) minus_avg_delta, count(delta) minus_cnt_delta
from public.ticker_price_delta_fact
where sequence_number >= (SELECT last_value-{getting_interval} FROM sequence_main)
      and delta < 0
group by ticker_name
) delta_minus
on delta_total.ticker_name = delta_minus.ticker_name
where total_avg_delta >= {alarm_threshold} and plus_cnt_delta >= 7
"""
minus_price_delta_select_query = f"""
select  delta_total.ticker_name ticker_name
	, coalesce(total_avg_delta,0) total_avg_delta, coalesce(total_cnt_delta,0) total_cnt_delta, coalesce(plus_avg_delta,0) plus_avg_delta
	, coalesce(plus_cnt_delta,0) plus_cnt_delta, coalesce(minus_avg_delta,0) minus_avg_delta, coalesce(minus_cnt_delta,0) minus_cnt_delta
    ,get_code('market',delta_total.ticker_name) korean_name
from
(
select ticker_name, round(cast(avg(delta) as numeric),1) total_avg_delta, count(delta) total_cnt_delta
from public.ticker_price_delta_fact
where sequence_number >= (SELECT last_value-{getting_interval} FROM sequence_main)
group by ticker_name
) delta_total
left join (
select ticker_name, round(cast(avg(delta) as numeric),1) plus_avg_delta, count(delta) plus_cnt_delta
from public.ticker_price_delta_fact
where sequence_number >= (SELECT last_value-{getting_interval} FROM sequence_main)
      and delta > 0
group by ticker_name
) delta_plus
on delta_total.ticker_name = delta_plus.ticker_name
left join (
select ticker_name, round(cast(avg(delta) as numeric),1) minus_avg_delta, count(delta) minus_cnt_delta
from public.ticker_price_delta_fact
where sequence_number >= (SELECT last_value-{getting_interval} FROM sequence_main)
      and delta < 0
group by ticker_name
) delta_minus
on delta_total.ticker_name = delta_minus.ticker_name
where total_avg_delta <= -{alarm_threshold} and minus_cnt_delta >= 7
"""
conn = psycopg2.connect(host='localhost', dbname='botdb', user='coinbot', password=os.environ['db_password'], port='5432')
conn.autocommit = True
cur = conn.cursor()
slack_token = os.environ['xoxb']
client = WebClient(token=slack_token)

def insert_code_group(group_name, code_key, code_value_char_1, code_value_int_1, code_value_float_1):
    insert_code_group_query = f"""
    insert into code_group (group_name, code_key, code_value_char_1, code_value_int_1, code_value_float_1)
    values ('{group_name}', '{code_key}', '{code_value_char_1}', {code_value_int_1}, {code_value_float_1})
    """
    logger.info(insert_code_group_query)
    cur.execute(insert_code_group_query)
    conn.commit()

def insert_message_log_log(sequence_number, message_type, message):
    insert_message_log_log_query = f"""
    insert into message_log (sequence_number, message_type, message)
    values ('{sequence_number}', '{message_type}', '{message}')
    """
    logger.info(insert_message_log_log_query)
    cur.execute(insert_message_log_log_query)
    conn.commit()

def delete_auto_trade_market():
    delete_auto_trade_list_query = f"""
    delete from code_group where group_name ='auto_order'
    """
    logger.info(delete_auto_trade_list_query)
    cur.execute(delete_auto_trade_list_query)
    conn.commit()

while True:
    try:
        loop_count = loop_count + 1
        if loop_count > 5:
            delete_auto_trade_count = 0
            loop_count = 0

        cur.execute(plus_price_delta_select_query)
        logger.debug(plus_price_delta_select_query)
        alarm_list = cur.fetchall()
        logger.debug(alarm_list)
        for alarm in alarm_list:
            try:
                coin_message = f"{alarm[0]} 코인이 최근 {getting_interval}분간  {alarm[3]} % 등락이 있었습니다. https://news.google.com/search?q={alarm[7]}&hl=ko&gl=KR&ceid=KR:ko"
                print(coin_message)
                response = client.chat_postMessage(
                    channel="showmethemoney",
                    text="상승 중 : " + coin_message
                )
                insert_code_group('auto_order', alarm[0], 'on', 1, alarm[3])
            except SlackApiError as e:
                # You will get a SlackApiError if "ok" is False
                assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'

        cur.execute(minus_price_delta_select_query)
        logger.debug(minus_price_delta_select_query)
        alarm_list = cur.fetchall()
        logger.debug(alarm_list)
        for alarm in alarm_list:
            delete_auto_trade_count = delete_auto_trade_count + 1
            coin_message = f"{alarm[0]} 코인이 최근 {getting_interval}분간  {alarm[5]} % 등락이 있었습니다. https://news.google.com/search?q={alarm[7]}&hl=ko&gl=KR&ceid=KR:ko"
            print(coin_message)
            response = client.chat_postMessage(
                channel="showmethemoney",
                text="하락 중 : " + coin_message
            )
            logger.debug(coin_message)
        sleep(60)
    except Exception as ex:
        logger.error(str(ex))
        traceback.print_exc()

cur.close()
conn.close()

