import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename='logs/es.log', level='INFO')

STATE_FILE = os.getenv('STATE_FILE')
STATE_KEY = 'last_modified'
ES_INDEX = 'movies'

dsn = {
    'dbname': os.getenv('PG_DB'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.environ.get('PG_HOST'),
    'port': os.environ.get('PG_PORT'),
    'options': '-c search_path=content',
}

es_node = {
    'host': os.getenv('ELASTIC_HOST'),
    'port': os.getenv('ELASTIC_PORT'),
}
