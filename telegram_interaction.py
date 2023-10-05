import requests
import os

def send(text):
	token = str(os.environ.get('TELEGRAM_KEY'))
	params = {'chat_id': os.environ.get('TELEGRAM_CHAT_ID'), 'text': text, 'parse_mode': 'HTML'}
	resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(token), params)
	resp.raise_for_status()