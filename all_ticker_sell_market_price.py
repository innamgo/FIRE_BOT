import psycopg2
import pyupbit
import os
import time
from time import sleep
import logging.handlers
import logging
import traceback
log_handlers = [logging.handlers.RotatingFileHandler(filename='/home/coinbot/all_sell_hoon.txt', maxBytes=1024), logging.StreamHandler()]
logging.basicConfig(level = logging.INFO, format = '%(asctime)s [%(levelname)s] : %(message)s', handlers = log_handlers)
logger = logging.getLogger('auto_trade_logger')

upbit = pyupbit.Upbit(os.environ['accesskey'], os.environ['secretkey'])
user_id = 'hoonkim'
conn = psycopg2.connect(host='localhost', dbname='botdb', user='coinbot', password=os.environ['db_password'], port='5432')
conn.autocommit = True
cur = conn.cursor()

def select_auto_trade_except():
    select_auto_trade_except_query = f"""
    select code_key from code_group where group_name ='auto_order_except' and code_value_char_2 ='{user_id}'
    """
    logger.info(select_auto_trade_except_query)
    cur.execute(select_auto_trade_except_query)
    return cur.fetchall()

def get_element_include(source_list, match_attribute, find_value):
    return_value=0
    for source in source_list:
        if source[match_attribute] == find_value:
            return_value = return_value + 1
    return return_value

def get_element_value(source_list, match_attribute, find_value, return_attribute):
    return_value = ''
    for source in source_list:
        if source[match_attribute] == find_value:
            return_value = source[return_attribute]
            break
    return return_value

def insert_trade_transaction_log(transaction_type, market_code, order_result):
    if type(order_result) == type(''):
        insert_trade_transaction_log_query = f"""
        insert into trade_transaction_log (transaction_type, market_coin, response_json, user_id)
        values('{transaction_type}', '{market_code}', '{order_result.replace("'",'"')}', '{user_id}')
        """
        logger.info(insert_trade_transaction_log_query)
        cur.execute(insert_trade_transaction_log_query)
        conn.commit()
    elif type(order_result) == type([]):
        for order_item in order_result:
            insert_trade_transaction_log_query = f"""
                    insert into trade_transaction_log (transaction_type, market_coin, response_json, user_id)
                    values('{transaction_type}', '{market_code}', '{str(order_item[0]).replace("'", '"')}', '{user_id}')
                    """
            logger.info(insert_trade_transaction_log_query)
            cur.execute(insert_trade_transaction_log_query)
            conn.commit()
    else:
        insert_trade_transaction_log_query = f"""
        insert into trade_transaction_log (transaction_type, market_coin, response_json, user_id)
        values('{transaction_type}', '{market_code}', '{str(order_result).replace("'",'"')}', '{user_id}')
        """
        logger.info(insert_trade_transaction_log_query)
        cur.execute(insert_trade_transaction_log_query)
        conn.commit()
try:
    my_balance = upbit.get_balances()
    auto_trade_except = select_auto_trade_except()
    logger.info(auto_trade_except)
    for my_balance_item in my_balance:
        market_name = my_balance_item['unit_currency'] + '-' + my_balance_item['currency']
        skip_yn='no'
        for except_item in auto_trade_except:
            if market_name in except_item:
                skip_yn='yes'
                logger.info('exist except item : ' + market_name)
        if skip_yn == 'yes':
            continue
        logger.info('try sell : ' + market_name)
        non_trade_list = upbit.get_order(market_name)
        wait_buy_trade = get_element_include(non_trade_list, 'side', 'ask')  # 매수 미체결 개수
        wait_sell_trade = get_element_include(non_trade_list, 'side', 'bid')  # 매도 미체결 개수
        current_market_balance = upbit.get_balance(ticker=market_name)  # 현재 해당 코인 잔고 조회
        if wait_buy_trade > 0: #매수 미체결 주문이 있으면 취소
            cancel_uuid = get_element_value(non_trade_list, 'side', 'ask', 'uuid')
            cancel_order_result = upbit.cancel_order(cancel_uuid)
            insert_trade_transaction_log('cancel', market_name, cancel_order_result)
            #sleep(0.1)

        if wait_sell_trade > 0: #매도 미체결 주문이 있으면 취소
            cancel_uuid = get_element_value(non_trade_list, 'side', 'bid', 'uuid')
            cancel_order_result = upbit.cancel_order(cancel_uuid)
            insert_trade_transaction_log('cancel', market_name, cancel_order_result)

        sell_order_result = upbit.sell_market_order(market_name, current_market_balance)
        insert_trade_transaction_log('forced_sell', market_name, sell_order_result)

except Exception as ex:
    logger.error(str(ex))
    traceback.print_exc()

cur.close()
conn.close()
