import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(filename='logs/es.log', level='INFO')

STATE_FILE = os.getenv('STATE_FILE')
STATE_KEY = 'last_modified'
ES_INDEX = 'movies'

dsn = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.environ.get('POSTGRES_HOST'),
    'port': os.environ.get('POSTGRES_PORT'),
    'options': '-c search_path=content',
}

es_node = {
    'host': os.getenv('ELASTIC_HOST'),
    'port': os.getenv('ELASTIC_PORT'),
}
