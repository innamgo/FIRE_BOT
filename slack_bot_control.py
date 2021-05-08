import os
from slack_bolt import App
import psycopg2
import time
from time import sleep
import logging.handlers
import logging
import traceback
import pyupbit
log_handlers = [logging.handlers.RotatingFileHandler(filename='/home/coinbot/control_bot_log.txt', maxBytes=1024), logging.StreamHandler()]
logging.basicConfig(level = logging.INFO, format = '%(asctime)s [%(levelname)s] : %(message)s', handlers = log_handlers)
logger = logging.getLogger('auto_trade_logger')

conn = psycopg2.connect(host=os.environ['db_url'], dbname='botdb', user='coinbot', password=os.environ['db_password'], port='5432')
conn.autocommit = True
cur = conn.cursor()
cur.execute("select get_code('system_parameter','control_bot_token','hoonkim') accesskey, get_code('system_parameter','control_bot_signing_secret','hoonkim') secretkey")
slack_key = cur.fetchall()
print(slack_key)
# Initializes your app with your bot token and signing secret
app = App(
    token=slack_key[0][0],
    signing_secret=slack_key[0][1]
)
cur.execute("select get_code('system_parameter','accesskey','hoonkim') accesskey, get_code('system_parameter','secretkey','hoonkim') secretkey")
upbit_key = cur.fetchall()
print(upbit_key)
upbit = pyupbit.Upbit(upbit_key[0][0], upbit_key[0][1])
@app.message("hello")
def message_hello(message, say):
    # say() sends a message to the channel where the event was triggered
    logger.info(message)
    say(
        blocks=[
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"Hey there <@{message['user']}>!"},
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Click Me"},
                    "action_id": "button_click"
                }
            }
        ],
        text=f"Hey there <@{message['user']}>!"
    )

@app.action("button_click")
def action_button_click(body, ack, say):
    # Acknowledge the action
    ack()
    say(f"<@{body['user']['id']}> clicked the button..버튼을 눌렀군..")

@app.action("approve_button")
def approve_request(ack, say):
    # Acknowledge action request
    ack()
    say("Request approved 👍")

@app.command("/echo")
def repeat_text(ack, say, command):
    # Acknowledge command request
    ack()
    say(f"버튼을 눌렀군...{command['text']}")

@app.message("help")
def message_help(message, say):
    say(f"cmd$update_max_money$금액$사용자 아이디 : 자동매매 최대 금액을 갱신")
    say(f"cmd$on_auto_trade$사용자 아이디 : 자동매매 프로그램을 시작")
    say(f"cmd$off_auto_trade$사용자 아이디 : 자동매매 프로그램을 중지")
    say(f"cmd$all_sell$사용자 아이디 : 자동매매 프로그램을 중지")


def select_auto_trade_except(user_id):
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

def insert_trade_transaction_log(transaction_type, market_code, order_result,user_id):
    try:
        if type(order_result) == type(''):
            insert_trade_transaction_log_query = f"""
            insert into trade_transaction_log (transaction_type, market_coin, response_json, user_id)
            values('{transaction_type}', '{market_code}', '{order_result.replace("'",'"').replace("None",'"None"')}', '{user_id}')
            """
            logger.info(insert_trade_transaction_log_query)
            cur.execute(insert_trade_transaction_log_query)
            conn.commit()
        elif type(order_result) == type([]):
            for order_item in order_result:
                insert_trade_transaction_log_query = f"""
                        insert into trade_transaction_log (transaction_type, market_coin, response_json, user_id)
                        values('{transaction_type}', '{market_code}', '{str(order_item[0]).replace("'", '"').replace("None",'"None"')}', '{user_id}')
                        """
                logger.info(insert_trade_transaction_log_query)
                cur.execute(insert_trade_transaction_log_query)
                conn.commit()
        else:
            insert_trade_transaction_log_query = f"""
            insert into trade_transaction_log (transaction_type, market_coin, response_json, user_id)
            values('{transaction_type}', '{market_code}', '{str(order_result).replace("'",'"').replace("None",'"None"')}', '{user_id}')
            """
            logger.info(insert_trade_transaction_log_query)
            cur.execute(insert_trade_transaction_log_query)
            conn.commit()
    except Exception as ex:
        logger.error(str(ex))
    traceback.print_exc()

@app.message("cmd")
def message_command(message, say):
    try:
        # say() sends a message to the channel where the event was triggered
        if 'cmd$update_max_money$' in message['text']:
            update_max_money = f"""
            update code_group set code_value_char_1 ='{message['text'].split('$')[2]}' where group_name ='system_parameter' and code_key ='max_money' and code_value_char_2 ='{message['text'].split('$')[3]}'
            """
            logger.info(update_max_money)
            cur.execute(update_max_money)
            conn.commit()
            say(f"{message['text'].split('$')[3]} 님의 자동 매매에 사용할 최대 금액을 {message['text'].split('$')[2]} 로 갱신 했습니다.")
        elif 'cmd$on_auto_trade$' in message['text']:
            on_auto_trade = f"""
            update code_group set code_value_char_1 ='on' where group_name ='system_parameter' and code_key ='is_run' and code_value_char_2 ='{message['text'].split('$')[2]}'
            """
            logger.info(on_auto_trade)
            cur.execute(on_auto_trade)
            conn.commit()
            say(f"{message['text'].split('$')[2]} 님의 자동 매매 프로그램을 시작 했습니다.")
        elif 'cmd$off_auto_trade$' in message['text']:
            off_auto_trade = f"""
            update code_group set code_value_char_1 ='off' where group_name ='system_parameter' and code_key ='is_run' and code_value_char_2 ='{message['text'].split('$')[2]}'
            """
            logger.info(off_auto_trade)
            cur.execute(off_auto_trade)
            conn.commit()
            say(f"{message['text'].split('$')[2]} 님의 자동 매매 프로그램을 중지 했습니다.")
        elif 'cmd$all_sell$' in message['text']:
            try:
                my_balance = upbit.get_balances()
                auto_trade_except = select_auto_trade_except(message['text'].split('$')[2])
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
                    if market_name == 'KRW-KRW':
                        continue

                    logger.info('try sell : ' + market_name)
                    non_trade_list = upbit.get_order(market_name)
                    logger.info(non_trade_list)
                    sleep(0.2)
                    wait_buy_trade = get_element_include(non_trade_list, 'side', 'bid')  # 매수 미체결 개수
                    wait_sell_trade = get_element_include(non_trade_list, 'side', 'ask')  # 매도 미체결 개수
                    current_market_balance = upbit.get_balance(ticker=market_name)  # 현재 해당 코인 잔고 조회
                    if wait_buy_trade > 0:  # 매수 미체결 주문이 있으면 취소
                        cancel_uuid = get_element_value(non_trade_list, 'side', 'bid', 'uuid')
                        cancel_order_result = upbit.cancel_order(cancel_uuid)
                        insert_trade_transaction_log('cancel_buy', market_name, cancel_order_result, message['text'].split('$')[2])

                    if wait_sell_trade > 0:  # 매도 미체결 주문이 있으면 취소
                        cancel_uuid = get_element_value(non_trade_list, 'side', 'ask', 'uuid')
                        cancel_order_result = upbit.cancel_order(cancel_uuid)
                        insert_trade_transaction_log('cancel_sell', market_name, cancel_order_result, message['text'].split('$')[2])

                    logger.info('bal:' + str(current_market_balance))
                    sell_order_result = upbit.sell_market_order(market_name, current_market_balance)
                    insert_trade_transaction_log('forced_sell', market_name, sell_order_result, message['text'].split('$')[2])
                say(f"{message['text'].split('$')[2]} 님의 전체 매도를 시도 했습니다.")
            except Exception as ex:
                logger.error(str(ex))
                traceback.print_exc()
    except Exception as ex:
        logger.error(str(ex))
        traceback.print_exc()
# Start your app
if __name__ == "__main__":
    app.start(port=3030)