import os
import pyupbit
import time
from time import sleep
import psycopg2

start = time.time()
getting_interval = 60
alarm_threshold = 1.00 # 1분간 상승 또는 하락 비율이 1% 이상일 경우
price_delta_insert_query = f"""
insert into public.ticker_price_delta_fact(
    select recent.sequence_number, recent.ticker_name, round(cast((recent.price - previous.price)/recent.price*100.00 as numeric), 1) as threshold
    from 
    (
        select * from ticker_price_fact tpf 
        where sequence_number = (SELECT last_value FROM sequence_main)
    ) recent
    join
    (
        select * from ticker_price_fact 
        where sequence_number = (SELECT last_value-1 FROM sequence_main)
    ) previous
    on recent.ticker_name = previous.ticker_name
)
"""
#업비트 티커 코드 조회
tickers = pyupbit.get_tickers(fiat="KRW")
print(tickers)
conn = psycopg2.connect(host='localhost', dbname='botdb', user='coinbot', password=os.environ['db_password'], port='5432')
cur = conn.cursor()

#티커별 현재가 조회
while True:
    cur.execute("SELECT NEXTVAL('sequence_main');")
    sequence_number = cur.fetchone()
    for ticker in tickers:
        price = pyupbit.get_current_price(ticker)
        sleep(0.1)
        print(ticker + ' : '+ str(price))
        cur.execute("INSERT INTO ticker_price_fact (sequence_number, ticker_name, price) VALUES (%s, %s, %s)", (sequence_number, ticker, price))
        conn.commit()
    cur.execute(price_delta_insert_query)
    conn.commit()
    sleep(getting_interval)

cur.close()
conn.close()

