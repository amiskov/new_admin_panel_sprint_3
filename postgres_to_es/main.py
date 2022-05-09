import logging
import time
from datetime import datetime
from typing import Optional

from elasticsearch import Elasticsearch

from config import STATE_FILE, STATE_KEY
from es_loader import connect_elastic, save_to_elastic, transform_pg_to_es
from pg_extractor import connect_pg, extract_from_pg
from psycopg2.extras import RealDictCursor  # type: ignore
from state import JsonFileStorage, State

log = logging.getLogger('Main')


def run_pipeline(pg_cursor: RealDictCursor,
                 es_client: Elasticsearch,
                 latest_processed_date: Optional[str]) -> None:
    """
    Runs the recursive process for retrieving/transforming/saving data
    from Postgres DB to Elastic node.
    """
    try:
        pg_data = extract_from_pg(pg_cursor,
                                  latest_processed_date,
                                  batch=10,
                                  query_limit=100)
        if not pg_data:
            log.info(
                f'{datetime.now()} Export has been successfully finished.\n\n')
            return

        es_data = transform_pg_to_es(pg_data)
        save_to_elastic(es_client, es_data)
    except Exception as err:
        log.error(
            f'{datetime.now()} Failed while running the pipeline.\n{err}\n\n')
        raise

    # Update the date of the last processed record
    next_last_modified = dict(pg_data[-1]).get('modified')
    state.set_state(STATE_KEY, str(next_last_modified))

    time.sleep(.3)  # for debugging purposes

    run_pipeline(pg_cursor, es_client, next_last_modified)


if __name__ == '__main__':
    state = State(JsonFileStorage(STATE_FILE))
    current_state = state.get_state(STATE_KEY)
    if not current_state:
        state.set_state(STATE_KEY, str(datetime.min))
        last_modified = state.get_state(STATE_KEY)
    else:
        last_modified = current_state

    def main():
        pg_cursor = connect_pg()
        es_client = connect_elastic()

        try:
            run_pipeline(pg_cursor, es_client, last_modified)

            # Clear the state after finishing the export
            state.set_state(STATE_KEY, str(datetime.min))
        except Exception as pg_err:
            log.error(f'Failed while running '
                      f'the pipeline.\n{type(pg_err)}: {pg_err}\n\n')
            raise

        pg_cursor.close()
        es_client.close()

    main()
