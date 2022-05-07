import json
import logging
from datetime import datetime
from typing import Callable, Optional

from elasticsearch import Elasticsearch
from psycopg2.extras import RealDictRow

from config import ES_INDEX, es_node

log = logging.getLogger('Elastic')


def get_elastic_schema(file_path: str) -> dict:
    """
    Returns the schema for the ElasticSearch index as a Python dictionary.
    Loads it from the given JSON file.
    """
    with open(file_path, 'r') as f:
        content = f.read()

        if not content:
            raise Exception(f'Schema file {file_path} not found.')

        return json.loads(content)


def transform_pg_to_es(pg_data: list[RealDictRow]) -> list[dict]:
    prepared_data: list[dict] = []

    def is_writer(person: dict) -> bool:
        return person.get('person_role') == 'writer'

    def is_director(person: dict) -> bool:
        return person.get('person_role') == 'director'

    def get_names(persons: list[dict]) -> list[Optional[str]]:
        def get_name(person: dict) -> Optional[str]:
            return person.get('name')

        return list(map(get_name, persons))

    def is_actor(person: dict) -> bool:
        return person.get('person_role') == 'actor'

    def get_persons(persons: list[dict], pred: Callable) -> list[dict]:
        def strip_fields(p: dict) -> dict:
            return {'id': p.get('person_id'),
                    'name': p.get('person_name')}

        return list(map(strip_fields, filter(pred, persons)))

    def get_director_name(persons: list[dict]) -> str:
        directors = list(filter(is_director, persons))
        director = list(filter(is_director, persons))[0] \
            if len(directors) else {}
        return director.get('person_name') or ''

    try:
        for row in pg_data:
            persons = row.get('persons')
            actors = get_persons(persons, is_actor)
            writers = get_persons(persons, is_writer)
            prepared_row = {
                'id': row.get('id'),
                'imdb_rating': row.get('rating'),
                'genre': row.get('genres'),
                'title': row.get('title'),
                'description': row.get('description'),
                'director': get_director_name(persons),
                'actors_names': get_names(actors),
                'writers_names': get_names(writers),
                'actors': actors,
                'writers': writers,
            }
            row_index = {
                'index': {
                    '_index': ES_INDEX,
                    '_id': row.get('id')
                }
            }

            prepared_data.append(row_index)
            prepared_data.append(prepared_row)
    except Exception as err:
        log.error(
            f'{datetime.now()} Failed while transforming the data.\n{err}\n\n')
        raise

    return prepared_data


def save_to_elastic(es_client: Elasticsearch, es_data: list[dict]) -> None:
    try:
        es_client.bulk(index=ES_INDEX, body=es_data, refresh=True)
    except Exception as err:
        log.error(f'{datetime.now()} Failed while saving'
                  f'to ElasticSearch.\n{err}\n\n')
        raise


def connect_elastic() -> Elasticsearch:
    es_client = Elasticsearch([es_node])

    log.info(f'\n{datetime.now()} Successfully connected to ElasticSearch '
             f'node {es_node.get("host")}:{es_node.get("port")}.')

    schema = get_elastic_schema('resources/es_schema.json')
    if not es_client.indices.exists(index=ES_INDEX):
        es_client.indices.create(index=ES_INDEX, body=schema)
        log.info(
            f'{datetime.now()} Index {ES_INDEX} was successfully created.')
    else:
        log.warning(f'{datetime.now()} Index {ES_INDEX} is already created.')

    return es_client
