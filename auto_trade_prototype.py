import psycopg2
import pyupbit
import os
import time
from time import sleep
import logging.handlers
import logging
import traceback

log_handlers = [logging.handlers.RotatingFileHandler(filename='/home/coinbot/log_hoon.txt', maxBytes=1024),
                logging.StreamHandler()]
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] : %(message)s', handlers=log_handlers)
logger = logging.getLogger('auto_trade_logger')

# max_buy_limit = 20000
max_sell_limit_rate = 1.04
max_buy_limit_rate = 1.0
max_auto_trade_sceond = 60 * 60 * 3
loop_auto_trade_second = 0
wait_second = 10
# my_money = 1000000
user_id = 'hoonkim'
conn = psycopg2.connect(host=os.environ['db_url'], dbname='botdb', user='coinbot', password=os.environ['db_password'],
                        port='5432')
conn.autocommit = True
cur = conn.cursor()
cur.execute(
    "select get_code('system_parameter','accesskey','hoonkim') accesskey, get_code('system_parameter','secretkey','hoonkim') secretkey")
upbit_key = cur.fetchall()
print(upbit_key)
upbit = pyupbit.Upbit(upbit_key[0][0], upbit_key[0][1])


def get_tick_size(price, increase):
    if price >= 2000000:
        tick_size = round(price * increase / 1000) * 1000
    elif price >= 1000000:
        tick_size = round(price * increase / 500) * 500
    elif price >= 500000:
        tick_size = round(price * increase / 100) * 100
    elif price >= 100000:
        tick_size = round(price * increase / 50) * 50
    elif price >= 10000:
        tick_size = round(price * increase / 10) * 10
    elif price >= 1000:
        tick_size = round(price * increase / 5) * 5
    elif price >= 100:
        tick_size = round(price * increase / 1) * 1
    elif price >= 10:
        tick_size = round(price * increase / 0.1) * 0.1
    else:
        tick_size = round(price * increase / 0.01) * 0.01
    return tick_size


def get_element_include(source_list, match_attribute, find_value):
    return_value = 0
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


def select_auto_trade_except():
    select_auto_trade_except_query = f"""
    select code_key from code_group where group_name ='auto_order_except' and code_value_char_2 ='{user_id}'
    union
    select code_key from code_group where group_name ='auto_order' and create_date > current_timestamp - '30 minutes'::interval
    and code_key not in (select code_key from code_group where group_name ='auto_order_except' and code_value_char_2 ='{user_id}' )
    group by code_key having count(*) > 1 and avg(code_value_float_1) >= 0.5
    """
    logger.info(select_auto_trade_except_query)
    cur.execute(select_auto_trade_except_query)
    return cur.fetchall()


auto_trade_list_query = f"""
select code_key, count(*) cnt, sum(code_value_float_1) sum_delta, avg(code_value_float_1) avg_delta from code_group where group_name ='auto_order' and create_date > current_timestamp - '30 minutes'::interval
and code_key not in (select code_key from code_group where group_name ='auto_order_except' and code_value_char_2 ='{user_id}' ) 
group by code_key having count(*) > 1 and avg(code_value_float_1) >= 0.5 
"""
system_parameter_query = f"""
select get_code('system_parameter','max_buy_limit','hoonkim') max_buy_limit, get_code('system_parameter','max_money','hoonkim') max_money, get_code('system_parameter','is_run','hoonkim') is_run
"""
all_sell_detect_query = f"""
select count(*)
	,(select sum(delta) from ticker_price_delta_fact 
		where create_date > current_timestamp - '60 minutes'::interval and ticker_name='KRW-BTC'
		group by  ticker_name)
	,(select sum(delta) from ticker_price_delta_fact 
		where create_date > current_timestamp - '60 minutes'::interval and ticker_name='KRW-ETH'
		group by  ticker_name)
from (
select ticker_name, sum(delta) from ticker_price_delta_fact 
where create_date > current_timestamp - '60 minutes'::interval
group by  ticker_name having sum(delta) < 0 
) auto_off
"""
auto_off_query = f"""
update code_group set code_value_char_1 ='off' where group_name ='system_parameter' and code_key ='is_run' and code_value_char_2 ='{user_id}'
"""
auto_on_query = f"""
update code_group set code_value_char_1 ='on' where group_name ='system_parameter' and code_key ='is_run' and code_value_char_2 ='{user_id}'
"""


def delete_auto_trade_market():
    delete_auto_trade_list_query = f"""
    delete from code_group where group_name ='auto_order' and create_date < current_timestamp - '30 minutes'::interval
    """
    logger.info(delete_auto_trade_list_query)
    cur.execute(delete_auto_trade_list_query)
    conn.commit()


def insert_trade_transaction_log(transaction_type, market_code, order_result):
    if type(order_result) == type(''):
        insert_trade_transaction_log_query = f"""
        insert into trade_transaction_log (transaction_type, market_coin, response_json, user_id)
        values('{transaction_type}', '{market_code}', '{order_result.replace("'", '"')}', '{user_id}')
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
        values('{transaction_type}', '{market_code}', '{str(order_result).replace("'", '"')}', '{user_id}')
        """
        logger.info(insert_trade_transaction_log_query)
        cur.execute(insert_trade_transaction_log_query)
        conn.commit()


def all_sell():
    try:
        my_balance_wallet = upbit.get_balances()

        for my_balance_ele in my_balance_wallet:
            ticker_name = my_balance_ele['unit_currency'] + '-' + my_balance_ele['currency']
            if ticker_name == 'KRW-KRW':
                continue
            logger.info('try sell : ' + ticker_name)
            ordered_item = upbit.get_order(ticker_name)
            logger.info(ordered_item)
            sleep(0.2)
            wait_buy = get_element_include(ordered_item, 'side', 'bid')  # 매수 미체결 개수
            wait_sell = get_element_include(ordered_item, 'side', 'ask')  # 매도 미체결 개수
            current_balance = upbit.get_balance(ticker=ticker_name)  # 현재 해당 코인 잔고 조회
            if wait_buy > 0:  # 매수 미체결 주문이 있으면 취소
                cancel_uuid = get_element_value(ordered_item, 'side', 'bid', 'uuid')
                cancel_order_result = upbit.cancel_order(cancel_uuid)
                insert_trade_transaction_log('cancel_buy', ticker_name, cancel_order_result)

            if wait_sell > 0:  # 매도 미체결 주문이 있으면 취소
                cancel_uuid = get_element_value(ordered_item, 'side', 'ask', 'uuid')
                cancel_order_result = upbit.cancel_order(cancel_uuid)
                insert_trade_transaction_log('cancel_sell', ticker_name, cancel_order_result)

            logger.info('bal:' + str(current_balance))
            sell_order_result = upbit.sell_market_order(ticker_name, current_balance)
            insert_trade_transaction_log('forced_sell', ticker_name, sell_order_result)

    except Exception as ex:
        logger.error(str(ex))
        traceback.print_exc()


while True:
    try:
        # 시장 상황 조회, 폭락장이 탐지되면 다 판다.
        cur.execute(all_sell_detect_query)
        logger.info(all_sell_detect_query)
        all_sell_detect = cur.fetchall()
        logger.info(all_sell_detect)
        if all_sell_detect[0][0] >= 100 and all_sell_detect[0][1] <= -1.0 and all_sell_detect[0][2] <= -1.0:
            cur.execute(auto_off_query)
            conn.commit()
            logger.info('forced sell all item!!!')
            all_sell()
            all_sell()
            all_sell()

        # 시스템 파라메터 조회
        cur.execute(system_parameter_query)
        logger.info(system_parameter_query)
        system_parameter = cur.fetchall()
        # 자동 매매 대상 목록 조회
        if system_parameter[0][2] == 'on':
            cur.execute(auto_trade_list_query)
            logger.info(auto_trade_list_query)
            auto_trade_list = cur.fetchall()
            logger.info(auto_trade_list)
            # 미체결 목록 조회 후 반복 매수/매도
            for auto_trade in auto_trade_list:
                # delete_auto_trade_market()
                non_trade_list = upbit.get_order(auto_trade[0])
                krw_balance = upbit.get_balance(ticker='KRW')
                wait_buy_trade = get_element_include(non_trade_list, 'side', 'ask')  # 매수 미체결 개수
                wait_sell_trade = get_element_include(non_trade_list, 'side', 'bid')  # 매도 미체결 개수
                current_market_balance = upbit.get_balance(ticker=auto_trade[0])  # 현재 해당 코인 잔고 조회
                current_unit_price = pyupbit.get_current_price(auto_trade[0])
                if auto_trade[1] > 1 and auto_trade[1] <= 10:
                    max_sell_limit_rate = 1.05
                    max_buy_limit_rate = 1
                elif auto_trade[1] > 11 and auto_trade[1] <= 20:
                    max_sell_limit_rate = 1.05
                    max_buy_limit_rate = 1
                elif auto_trade[1] > 21 and auto_trade[1] <= 30:
                    max_sell_limit_rate = 1.04
                    max_buy_limit_rate = 0.99
                elif auto_trade[1] > 31 and auto_trade[1] <= 40:
                    max_sell_limit_rate = 1.04
                    max_buy_limit_rate = 0.99
                elif auto_trade[1] > 41:
                    max_sell_limit_rate = 1.03
                    max_buy_limit_rate = 0.98

                if wait_buy_trade > 0 and wait_sell_trade == 0 and current_market_balance == 0:  # 미체결 매도가 없고 미체결 매수 있고 잔고도 0이라면 있다면 아무것도 안함
                    pass
                elif wait_buy_trade == 0 and wait_sell_trade == 0 and current_market_balance > 0:  # 미체결 매도/매수가 없고 잔고가 있다면 매도
                    sell_unit_price = get_tick_size(pyupbit.get_current_price(auto_trade[0]), max_sell_limit_rate)
                    sell_order_result = upbit.sell_limit_order(auto_trade[0], sell_unit_price, current_market_balance)
                    insert_trade_transaction_log('sell', auto_trade[0], sell_order_result)
                elif wait_buy_trade == 0 and wait_sell_trade == 0 and current_market_balance == 0 and krw_balance > int(
                        system_parameter[0][1]):  # 미체결 매수/매도가 없다면 최대 정해진 금액 이하로 매수
                    buy_order_result = upbit.buy_limit_order(auto_trade[0],
                                                             get_tick_size(current_unit_price, max_buy_limit_rate),
                                                             round(int(system_parameter[0][0]) / current_unit_price, 6))
                    # logger.info('get_tick_size(current_unit_price,0.99) : ' + str(get_tick_size(current_unit_price,0.99)) + ', round(max_buy_limit / current_unit_price,6): ' + str(round(max_buy_limit / current_unit_price,6)))
                    # logger.info(buy_order_result)
                    insert_trade_transaction_log('buy', auto_trade[0], buy_order_result)
                elif wait_buy_trade == 0 and wait_sell_trade > 0 and current_market_balance == 0:  # 미체결 매수가 없고 매도만 있다면 정해진 시간이 지난 후 자동매매 대상에서 삭제 미체결 주문도 취소
                    loop_auto_trade_second = loop_auto_trade_second + wait_second
                    if loop_auto_trade_second >= max_auto_trade_sceond:
                        cancel_uuid = get_element_value(non_trade_list, 'side', 'bid', 'uuid')
                        cancel_order_result = upbit.cancel_order(cancel_uuid)
                        insert_trade_transaction_log('cancel', auto_trade[0], cancel_order_result)
                        # delete_auto_trade_market(auto_trade[0])
                        loop_auto_trade_second = 0
                else:  # 이건 무슨 상황
                    logger.info('wait_buy_trade : ' + str(wait_buy_trade))
                    logger.info('wait_sell_trade : ' + str(wait_sell_trade))
                    logger.info('current_market_balance : ' + str(current_market_balance))

            my_balance = upbit.get_balances()
            auto_trade_except = select_auto_trade_except()
            logger.info(auto_trade_except)
            for my_balance_item in my_balance:
                market_name = my_balance_item['unit_currency'] + '-' + my_balance_item['currency']
                skip_yn = 'no'
                for except_item in auto_trade_except:
                    if market_name in except_item:
                        skip_yn = 'yes'
                        logger.info('exist except item : ' + market_name)
                if skip_yn == 'yes':
                    continue
                logger.info('try sell : ' + market_name)
                if float(my_balance_item['locked']) == 0.0:
                    sleep(0.1)
                    current_unit_price = pyupbit.get_current_price(market_name)
                    if current_unit_price != None and (
                            current_unit_price * float(my_balance_item['balance'])) >= 5000 and (
                            current_unit_price - float(
                        my_balance_item['avg_buy_price'])) / current_unit_price * 100 >= 3.0:
                        sell_order_result = upbit.sell_limit_order(market_name, current_unit_price,
                                                                   float(my_balance_item['balance']))
                        insert_trade_transaction_log('remain_sell', market_name, sell_order_result)
                    else:
                        pass

            logger.info('waiting seconds : ' + str(wait_second))
            sleep(wait_second)
        else:
            logger.info('system parameter off, waiting seconds : ' + str(wait_second))
            sleep(wait_second)
            # 시장 상황 조회, 폭락장에서 정상화 되고 있다면 다시 켠다.
            cur.execute(all_sell_detect_query)
            logger.info(all_sell_detect_query)
            all_sell_detect = cur.fetchall()
            if all_sell_detect[0][0] < 85 and all_sell_detect[0][1] >= -0.5 and all_sell_detect[0][2] >= -0.5:
                cur.execute(auto_on_query)
                conn.commit()
                logger.info('auto trade on!!!')

    except Exception as ex:
        logger.error(str(ex))
        traceback.print_exc()

cur.close()
conn.close()
"""
#ret = upbit.buy_limit_order("KRW-XRP", 50, 100)
#print(ret)
#ret = upbit.sell_limit_order("KRW-XRP", 100000, 1)
#print(ret)
#ret = upbit.sell_limit_order("KRW-XRP", 1000, 20)
print(upbit.get_balance(ticker='KRW-XRP'))
#print(pyupbit.get_orderbook(tickers="KRW-BTC"))
ret=upbit.get_order("KRW-XRP")
print(ret)
#print(upbit.cancel_order(ret[0]['uuid']))
print(pyupbit.get_current_price("KRW-XRP"))
"""
