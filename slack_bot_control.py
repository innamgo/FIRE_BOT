import os
from slack_bolt import App
import psycopg2
import time
from time import sleep
import logging.handlers
import logging
import traceback
from flask import Flask, redirect, url_for, request
from flask import Response
import json
app = Flask(__name__)

@app.route('/slack', methods=['POST','GET'])
def slack():
    payload = request.get_data()
    data = json.loads(payload)
    return Response(data["challenge"], mimetype='application/x-www-form-urlencoded')

log_handlers = [logging.handlers.RotatingFileHandler(filename='/home/coinbot/slack_bot_log.txt', maxBytes=1024), logging.StreamHandler()]
logging.basicConfig(level = logging.INFO, format = '%(asctime)s [%(levelname)s] : %(message)s', handlers = log_handlers)
logger = logging.getLogger('auto_trade_logger')

conn = psycopg2.connect(host=os.environ['db_url'], dbname='botdb', user='coinbot', password=os.environ['db_password'], port='5432')
conn.autocommit = True
cur = conn.cursor()
cur.execute("select get_code('system_parameter','coin_bot_1.0_token','hoonkim') accesskey, get_code('system_parameter','coin_bot_1.0_signing_secret','hoonkim') secretkey")
slack_key = cur.fetchall()
print(slack_key)
# Initializes your app with your bot token and signing secret
app = App(
    token=slack_key[0][0],
    signing_secret=slack_key[0][1]
)
@app.message("hello")
def message_hello(message, say):
    # say() sends a message to the channel where the event was triggered
    logger.info('ggg')
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

@app.message("-help")
def message_hello(message, say):
    # say() sends a message to the channel where the event was triggered
    say(f"아직 이벤트 핸들러 안만들었어...")

# Start your app
if __name__ == "__main__":
    app.start(port=3030)