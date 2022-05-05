import json
import logging
import time
from datetime import datetime

import psycopg2
from elasticsearch import Elasticsearch

from config import STATE_FILE, dsn, elastic
from pg_extractor import extract_from_pg
from state import JsonFileStorage, State


def get_schema(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

        if not content:
            raise Exception("Schema file not found.")

        return json.loads(content)


log = logging.getLogger('MainLog')

columns = ['id', 'title', 'description', 'imdb_rating',
           'genre', 'director', 'actors_names', 'writers_names',
           'actors', 'writers']
batch = 10
state = State(JsonFileStorage(STATE_FILE))
STATE_KEY = 'last_modified'

client = Elasticsearch(elastic)
INDEX = 'movies'


def load_data_es(query: list) -> None:
    data_json = json.dumps(query, default=str)
    load_json = json.loads(data_json)
    data = []

    for row in load_json:
        for i in row:
            if row[i] is None:
                row[i] = []

        data.append({"create": {"_index": "movies", "_id": row['id']}})
        data.append(row)
        client.bulk(index='movies', body=data, refresh=True)
        data.clear()


def save_to_elastic(pg_records: list) -> None:
    count_records = len(pg_records)
    index = 0
    block = []

    while count_records != 0:
        if count_records >= batch:
            for row in pg_records[index: index + batch]:
                print(row)
                block.append(dict(zip(columns, row)))
                index += 1
            count_records -= batch
            load_data_es(block)
            block.clear()
        else:
            load_data_es([dict(zip(columns, row))
                          for row in pg_records[index: index + count_records]])
            count_records -= count_records


def run_data_processing(pg_cursor, latest_processed_date):
    pg_data = extract_from_pg(pg_cursor, latest_processed_date)

    save_to_elastic(pg_data)

    # Update the date of the last processed record
    next_last_modified = pg_data[-1][5]
    state.set_state(STATE_KEY, str(next_last_modified))

    time.sleep(1)

    run_data_processing(pg_cursor, next_last_modified)


if __name__ == '__main__':
    last_modified = state.get_state(STATE_KEY)

    if not last_modified:
        state.set_state(STATE_KEY, str(datetime.min))
        last_modified = state.get_state(STATE_KEY)

    schema = get_schema('resources/es_schema.json')
    if not client.indices.exists(INDEX):
        client.indices.create(index=INDEX, body=schema)
        log.info(f'{datetime.now()} Index {INDEX} was successfully created.')
    else:
        log.warning(f'{datetime.now()} Index {INDEX} is already created.')

    with psycopg2.connect(**dsn) as conn, conn.cursor() as cur:
        log.info(f'{datetime.now()} Successfully connected to Postgres.')

        # @backoff() for recursive function???
        run_data_processing(cur, last_modified)
