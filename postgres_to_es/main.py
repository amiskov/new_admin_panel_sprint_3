import logging
import time
from datetime import datetime

from config import STATE_FILE, STATE_KEY
from pg_extractor import extract_from_pg, connect_pg
from state import JsonFileStorage, State
from backoff import backoff
from es_loader import transform_pg_to_es, save_to_elastic, connect_elastic

log = logging.getLogger('Main')


def run_pipeline(pg_cursor, es_client, latest_processed_date) -> None:
    """
    Runs the recursive process for retrieving/transforming/saving data from Postgres DB to Elastic node.
    """
    try:
        pg_data = extract_from_pg(pg_cursor, latest_processed_date, batch=10, query_limit=100)
        if not pg_data:
            log.info(f'{datetime.now()} Export has been successfully finished.\n\n')
            return

        es_data = transform_pg_to_es(pg_data)
        save_to_elastic(es_client, es_data)
    except Exception as err:
        log.error(f'{datetime.now()} Failed while running the pipeline.\n{err}\n\n')
        raise

    # Update the date of the last processed record
    next_last_modified = dict(pg_data[-1]).get('modified')
    state.set_state(STATE_KEY, str(next_last_modified))

    time.sleep(3)

    run_pipeline(pg_cursor, es_client, next_last_modified)


if __name__ == '__main__':
    state = State(JsonFileStorage(STATE_FILE))
    current_state = state.get_state(STATE_KEY)
    if not current_state:
        state.set_state(STATE_KEY, str(datetime.min))
        last_modified = state.get_state(STATE_KEY)
    else:
        last_modified = current_state


    @backoff()
    def main():
        pg_cursor = connect_pg()
        es_client = connect_elastic()

        try:
            run_pipeline(pg_cursor, es_client, last_modified)
            state.set_state(STATE_KEY, str(datetime.min))  # Clear the state after finishing the export
        except Exception as pg_err:
            log.error(f'Failed while running the pipeline.\n{type(pg_err)}: {pg_err}\n\n')
            raise


    main()
