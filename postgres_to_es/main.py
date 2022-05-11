import logging
import time
from datetime import datetime
from typing import Generator, Any, Tuple, Optional

from elasticsearch import Elasticsearch

from config import STATE_FILE
from es_loader import connect_elastic, save_to_elastic, transform_pg_to_es
from pg_extractor import connect_pg, get_film_works, get_entity_ids, \
    get_film_work_ids
from psycopg2.extras import RealDictCursor  # type: ignore
from state import JsonFileStorage, State

log = logging.getLogger('Main')

connections = {
    'pg_cursor': None,
    'es_client': None,
    'state': None
}


def get_connections():
    """
    Returns available connections to Postgres, Elastic, and State.
    Tries to reconnect (using backoff-ed functions) if they're not available.
    """
    global connections

    pg_cursor = connections['pg_cursor']
    es_client = connections['es_client']
    state = connections['state']

    is_pg_alive = pg_cursor and (not pg_cursor.closed)
    is_es_alive = es_client and es_client.ping()
    is_state_alive = bool(state)

    if not is_pg_alive:
        connections['pg_cursor'] = connect_pg(failed_cursor=pg_cursor)
    if not is_es_alive:
        connections['es_client'] = connect_elastic(failed_clent=es_client)
    if not is_state_alive:
        connections['state'] = State(JsonFileStorage(STATE_FILE))

    return connections['pg_cursor'], connections['es_client'], connections[
        'state']


def run_pipeline(table_name: str) -> None:
    """
    Runs the process for retrieving/transforming/saving data from Postgres
    to Elastic.
    """
    pg_cursor, es_client, state = get_connections()

    try:
        for entity_ids in get_entity_ids(pg_cursor, state, table_name):
            for film_work_ids in get_film_work_ids(pg_cursor, table_name, entity_ids):
                if not film_work_ids:
                    return
                for film_works in get_film_works(pg_cursor, film_work_ids):
                    es_data = transform_pg_to_es(film_works)
                    save_to_elastic(es_client, es_data)
    except Exception as pipeline_err:
        log.error(
            f'{datetime.now()} Failed while running the pipeline.'
            f'\n{pipeline_err}\n\n')


def cycle_through(lst: list[Any]) -> Generator[Any, None, None]:
    """
    Yields one list item at a time endlessly.
    """
    idx = 0
    while True:
        yield lst[idx]
        penultimate_idx = len(lst) - 1
        idx = idx + 1 if idx < penultimate_idx else 0


if __name__ == '__main__':
    try:
        # Run pipeline forever for each table at a time
        for table_name in cycle_through(['film_work', 'genre', 'person']):
            log.info(f'Exporting {table_name}...\n')
            run_pipeline(table_name)
            time.sleep(1)

    except Exception as err:
        log.error(f'Failed while running the pipeline.'
                  f'\n{type(err)}: {err}\n\n')
