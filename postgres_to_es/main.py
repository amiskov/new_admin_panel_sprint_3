import logging
import time
from datetime import datetime
from typing import Optional, Generator, Any

from elasticsearch import Elasticsearch

from config import STATE_FILE
from es_loader import connect_elastic, save_to_elastic, transform_pg_to_es
from pg_extractor import connect_pg, extract_from_pg
from psycopg2.extras import RealDictCursor  # type: ignore
from state import JsonFileStorage, State

log = logging.getLogger('Main')


def get_last_modified(state, table) -> str:
    current_state = state.get_state(table)
    if not current_state:
        state.set_state(table, str(datetime.min))
        last_modified = state.get_state(table)
    else:
        last_modified = current_state

    return last_modified


def run_pipeline(pg_cursor: RealDictCursor,
                 es_client: Elasticsearch,
                 state: State,
                 table_name: str) -> None:
    """
    Runs the process for retrieving/transforming/saving data
    from Postgres DB to Elastic node.
    """
    last_modified = get_last_modified(state, table_name)

    try:
        pg_data, next_last_modified = extract_from_pg(pg_cursor, table_name,
                                                      last_modified,
                                                      query_limit=100)
        if not pg_data:
            log.info(
                f'{datetime.now()} Table `{table_name}` '
                f'has been successfully exported.\n\n')
            return

        es_data = transform_pg_to_es(pg_data)
        save_to_elastic(es_client, es_data)
    except Exception as err:
        log.error(
            f'{datetime.now()} Failed while running the pipeline.\n{err}\n\n')
        raise

    # Update the date of the last processed record
    state.set_state(table_name, str(next_last_modified))


def cycle_through(l: list[Any]) -> Generator[Any, None, None]:
    """
    Yields one list item at a time endlessly.
    """
    idx = 0
    while True:
        yield l[idx]
        penultimate_idx = len(l) - 1
        idx = idx + 1 if idx < penultimate_idx else 0


if __name__ == '__main__':
    try:
        pg_cursor = connect_pg()
        es_client = connect_elastic()
        state = State(JsonFileStorage(STATE_FILE))

        # Run pipeline forever for each table at a time
        for table_name in cycle_through(['film_work', 'genre', 'person']):
            print(f'Exporting {table_name}...')
            run_pipeline(pg_cursor, es_client, state, table_name)
            time.sleep(1)

        pg_cursor.close()
        es_client.close()
    except Exception as err:
        log.error(f'Failed while running the pipeline.'
                  f'\n{type(err)}: {err}\n\n')
