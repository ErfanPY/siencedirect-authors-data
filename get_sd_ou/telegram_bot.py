import json

import dotenv
import os
import logging

import requests
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

dotenv.load_dotenv()
TOKEN = os.environ["TELEGRAM_TOKEN"]
SERVER_ADDRESS = os.environ["SERVER_ADDRESS"]

updater = Updater(token=TOKEN, use_context=True)
dispatcher = updater.dispatcher


def start(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text="Running!")


def status(update: Update, context: CallbackContext):
    status_data = requests.get(f"http://{SERVER_ADDRESS}/journals/status").content
    status_dict = json.loads(status_data)

    database_status = ""
    for key, value in status_dict.items():
        if value:
            database_status += f"{key}: {value}\n"

    context.bot.send_message(chat_id=update.effective_chat.id, text=str(database_status))


start_handler = CommandHandler('start', start)
dispatcher.add_handler(start_handler)

status_handler = CommandHandler('status', status)
dispatcher.add_handler(status_handler)

updater.start_polling()
