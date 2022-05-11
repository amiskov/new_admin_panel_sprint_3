import logging
import time
from datetime import datetime
from typing import Generator, Any

from elasticsearch import Elasticsearch

from config import STATE_FILE
from es_loader import connect_elastic, save_to_elastic, transform_pg_to_es
from pg_extractor import connect_pg, get_film_works, get_entity_ids, \
    get_film_work_ids
from psycopg2.extras import RealDictCursor  # type: ignore
from state import JsonFileStorage, State

log = logging.getLogger('Main')


def run_pipeline(pg_cursor: RealDictCursor,
                 es_client: Elasticsearch,
                 state: State,
                 table_name: str) -> None:
    """
    Runs the process for retrieving/transforming/saving data
    from Postgres DB to Elastic node.
    """

    try:
        for entity_ids in get_entity_ids(pg_cursor, state, table_name):
            for fw_ids in get_film_work_ids(pg_cursor, table_name, entity_ids):
                if not fw_ids:
                    log.info(f'''{datetime.now()} Table `{table_name}`
                        has been successfully exported.\n''')
                    return
                for film_works in get_film_works(pg_cursor, fw_ids):
                    es_data = transform_pg_to_es(film_works)
                    save_to_elastic(es_client, es_data)
    except Exception as pipeline_err:
        log.error(
            f'{datetime.now()} Failed while running the pipeline.'
            f'\n{pipeline_err}\n\n')
        raise


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
        pg_cursor = connect_pg()
        es_client = connect_elastic()
        state = State(JsonFileStorage(STATE_FILE))

        # Run pipeline forever for each table at a time
        for table_name in cycle_through(['film_work', 'genre', 'person']):
            log.info(f'Exporting {table_name}...\n')
            run_pipeline(pg_cursor, es_client, state, table_name)
            time.sleep(3)

        pg_cursor.close()
        es_client.close()
    except Exception as err:
        log.error(f'Failed while running the pipeline.'
                  f'\n{type(err)}: {err}\n\n')
