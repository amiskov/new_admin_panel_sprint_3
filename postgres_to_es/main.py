import json
import logging
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from elasticsearch import Elasticsearch

from config import STATE_FILE, dsn, es_node
from pg_extractor import extract_from_pg
from state import JsonFileStorage, State
from backoff import backoff

log = logging.getLogger('MAIN')

state = State(JsonFileStorage(STATE_FILE))
STATE_KEY = 'last_modified'

INDEX = 'movies'


def get_elastic_schema(file_path: str) -> dict:
    """
    Returns the schema for the ElasticSearch index as a Python dictionary. Loads it from the given JSON file.
    """
    with open(file_path, 'r') as f:
        content = f.read()

        if not content:
            raise Exception(f'Schema file {file_path} not found.')

        return json.loads(content)


def transform_pg_to_es(pg_data) -> list[dict]:
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
        director = list(filter(is_director, persons))[0] if len(directors) else {}
        return director.get('person_name')

    try:
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
    except Exception as err:
        log.error(f'{datetime.now()} Failed while transforming the data.\n{err}\n\n')
        raise

    return prepared_data


@backoff()
def save_to_elastic(es_client, es_data) -> None:
    try:
        es_client.bulk(index=INDEX, body=es_data, refresh=True)
    except Exception as err:
        log.error(f'{datetime.now()} Failed while saving to ElasticSearch.\n{err}\n\n')
        raise


def run_pipeline(pg_cursor, es_client, latest_processed_date) -> None:
    """
    Runes the recursive process for retrieving/transforming/saving data from Postgres DB to Elastic node.
    """
    try:
        pg_data = extract_from_pg(pg_cursor, latest_processed_date, batch=1, query_limit=1)
        es_data = transform_pg_to_es(pg_data)
        save_to_elastic(es_client, es_data)
    except Exception as err:
        log.error(f'{datetime.now()} Failed while running the pipeline.\n{err}\n\n')
        raise

    # Update the date of the last processed record
    next_last_modified = dict(pg_data[-1]).get('modified')
    state.set_state(STATE_KEY, str(next_last_modified))

    time.sleep(1)

    run_pipeline(pg_cursor, es_client, next_last_modified)


if __name__ == '__main__':
    last_modified = state.get_state(STATE_KEY)

    if not last_modified:
        state.set_state(STATE_KEY, str(datetime.min))
        last_modified = state.get_state(STATE_KEY)

    es_client = Elasticsearch([es_node])
    log.info(f'\n{datetime.now()} Successfully connected to '
             f'ElasticSearch node {es_node.get("host")}:{es_node.get("port")}.')
    schema = get_elastic_schema('resources/es_schema.json')
    if not es_client.indices.exists(INDEX):
        es_client.indices.create(index=INDEX, body=schema)
        log.info(f'{datetime.now()} Index {INDEX} was successfully created.')
    else:
        log.warning(f'{datetime.now()} Index {INDEX} is already created.')

    # TODO: Cursor will be closed if PG RDBMS will fail (try to switch it off in Docker).
    with psycopg2.connect(**dsn, cursor_factory=RealDictCursor) as conn, \
            conn.cursor() as cur:
        log.info(f'{datetime.now()} Successfully connected to Postgres.')

        run_pipeline(cur, es_client, last_modified)
