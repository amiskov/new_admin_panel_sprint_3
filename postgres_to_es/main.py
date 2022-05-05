import json
import logging
import time
from datetime import datetime
from pprint import pprint

import psycopg2
from psycopg2.extras import RealDictCursor
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

state = State(JsonFileStorage(STATE_FILE))
STATE_KEY = 'last_modified'

es_client = Elasticsearch(elastic)
INDEX = 'movies'


def transform(pg_data):
    prepared_data = []

    def is_writer(person):
        return person.get("person_role") == 'writer'

    def is_director(person):
        return person.get("person_role") == 'director'

    def get_names(persons):
        def get_name(person):
            return person.get("name")

        return list(map(get_name, persons))

    def is_actor(person):
        return person.get("person_role") == 'actor'

    def get_persons(persons, pred):
        def strip_fields(p):
            return {'id': p.get("person_id"),
                    'name': p.get('person_name')}

        return list(map(strip_fields, filter(pred, persons)))

    def get_director_name(persons):
        directors = list(filter(is_director, persons))
        director = list(filter(is_director, persons))[
            0] if len(directors) else {}
        return director.get('person_name')

    for row in pg_data:
        persons = row.get("persons")
        actors = get_persons(persons, is_actor)
        writers = get_persons(persons, is_writer)
        prepared_row = {
            "id": row.get("id"),
            "imdb_rating": row.get("rating"),
            "genre": ", ".join(row.get('genres')),
            "title": row.get("title"),
            "description": row.get("description"),
            "director": get_director_name(persons),
            "actors_names": get_names(actors),
            "writers_names": get_names(writers),
            "actors": actors,
            "writers": writers,
        }
        row_index = {"index": {"_index": INDEX, "_id": row.get('id')}}

        prepared_data.append(row_index)
        prepared_data.append(prepared_row)

    return prepared_data


def run_data_processing(pg_cursor, latest_processed_date):
    pg_data = extract_from_pg(
        pg_cursor, latest_processed_date, batch=1, limit=1)

    json_data = transform(pg_data)
    pprint(json_data)

    try:
        es_client.bulk(index=INDEX, body=json_data, refresh=True)
    except Exception as err:
        print(err)

    # Update the date of the last processed record
    next_last_modified = dict(pg_data[-1]).get('modified')
    print("next_last_modified", next_last_modified)
    state.set_state(STATE_KEY, str(next_last_modified))

    time.sleep(1)

    run_data_processing(pg_cursor, next_last_modified)


if __name__ == '__main__':
    last_modified = state.get_state(STATE_KEY)

    if not last_modified:
        state.set_state(STATE_KEY, str(datetime.min))
        last_modified = state.get_state(STATE_KEY)

    schema = get_schema('resources/es_schema.json')
    if not es_client.indices.exists(INDEX):
        es_client.indices.create(index=INDEX, body=schema)
        log.info(f'{datetime.now()} Index {INDEX} was successfully created.')
    else:
        log.warning(f'{datetime.now()} Index {INDEX} is already created.')

    with psycopg2.connect(**dsn, cursor_factory=RealDictCursor) as conn, \
            conn.cursor() as cur:
        log.info(f'{datetime.now()} Successfully connected to Postgres.')

        # @backoff() for recursive function???
        run_data_processing(cur, last_modified)
