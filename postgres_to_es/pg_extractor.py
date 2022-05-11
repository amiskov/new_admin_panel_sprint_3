import logging
from datetime import datetime
from typing import Any, Generator, Optional

import psycopg2  # type: ignore
from psycopg2.extras import RealDictCursor, RealDictRow  # type: ignore

from backoff import backoff
from config import dsn
from state import State

log = logging.getLogger('Postgres')


def to_query_str(lst: list[str]) -> str:
    """
    Returns a string for the inner part of an SQL-tuple.

    > to_query_str(['1', '2', '3'])
    "'1', '2', '3'"  # in SQL it will become `('1', '2', '3')`
    """
    return ', '.join("'" + i + "'" for i in lst)


def only_first_els(ts: list[tuple]) -> list[Any]:
    """
    Returns a list with first elements of tuples from the given list of tuples.

    > only_first_els([(1, "a"), (2, "b"), (3, "c")])
    [1, 2, 3]
    """
    return [el[0] for el in ts]


def entity_ids_query(table_name: str, from_modified: str):
    """
    Query has no `LIMIT`, should be used in generator.
    """
    return f"""
    SELECT id, modified
    FROM content.{table_name}
    WHERE modified > '{from_modified}'
    ORDER BY modified;
    """


def query_film_work_ids(table_name: str, ids: list[str]) -> str:
    """
    Query has no `LIMIT`, should be used in generator.
    """
    return f"""
    SELECT fw.id, fw.modified
    FROM content.film_work fw
    LEFT JOIN content.{table_name}_film_work tfw ON tfw.film_work_id = fw.id
    WHERE tfw.{table_name}_id IN ({to_query_str(ids)})
    ORDER BY fw.modified;
    """


def query_film_works(film_work_ids: list[str]) -> str:
    """
    Query has no `LIMIT`, should be used in generator.
    """
    return f"""
    SELECT
    fw.id,
    fw.title,
    fw.description,
    fw.rating,
    fw.type,
    fw.created,
    fw.modified,
    COALESCE (
        json_agg(
            DISTINCT jsonb_build_object(
               'person_role', pfw.role,
               'person_id', p.id,
               'person_name', p.full_name
            )
        ) FILTER (WHERE p.id is not null),
        '[]'
    ) as persons,
    array_agg(DISTINCT g.name) as genres
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.id IN ({to_query_str(film_work_ids)})
    GROUP BY fw.id
    ORDER BY fw.modified;
    """


@backoff()
def connect_pg(failed_cursor: Optional[RealDictCursor] = None):
    if failed_cursor:
        failed_cursor.close()
    try:
        conn = psycopg2.connect(**dsn, cursor_factory=RealDictCursor)
        log.info(f'{datetime.now()} Successfully connected to Postgres.')
        return conn.cursor()
    except Exception as err:
        log.error(f'{datetime.now()} Postgres connection failed.\n{err}\n\n')
        raise


def get_entity_state(state: State, entity_table_name: str) -> str:
    """
    Retrieves the state for the given entity.

    > get_entity_state(State(JsonFileStorage('state.json')), 'genre')
    '2022-05-11 13:05:31.142997+00:00'
    """
    current_state = state.get_state(entity_table_name)
    if not current_state:
        state.set_state(entity_table_name, str(datetime.min))
        return state.get_state(entity_table_name)
    else:
        return current_state


@backoff()
def get_entity_ids(pg_cursor: RealDictCursor,
                   state: State,
                   entity_table_name: str,
                   batch_size: int = 100
                   ) -> Generator[list[str], None, None]:
    try:
        while True:
            last_modified = get_entity_state(state, entity_table_name)
            pg_cursor.execute(
                entity_ids_query(entity_table_name, last_modified)
            )
            entity_records = pg_cursor.fetchmany(batch_size)
            if not entity_records:
                log.info(f'No modifications found in {entity_table_name}.')
                break
            yield [t['id'] for t in entity_records]
            next_last_modified = entity_records[-1].get('modified')
            state.set_state(entity_table_name, str(next_last_modified))
    except Exception as err:
        log.error(f'''{datetime.now()} Failed while extracting
            {entity_table_name} IDs.\n{err}\n\n''')
        raise


@backoff()
def get_film_work_ids(pg_cursor: RealDictCursor,
                      entity_table_name: str,
                      entity_ids: list[str],
                      batch_size: int = 100
                      ) -> Generator[list[str], None, None]:
    try:
        if entity_table_name == 'film_work':
            yield entity_ids
        else:
            pg_cursor.execute(
                query_film_work_ids(entity_table_name, entity_ids)
            )
            while True:
                film_work_records = pg_cursor.fetchmany(batch_size)
                if not film_work_records:
                    break
                yield [t['id'] for t in film_work_records]
    except Exception as err:
        log.error(
            f'{datetime.now()} Failed while extracting film_work IDs.'
            f'\n{err}\n\n')
        raise


@backoff()
def get_film_works(pg_cursor: RealDictCursor,
                   film_work_ids: list[str],
                   batch_size=100
                   ) -> Generator[list[RealDictRow], None, None]:
    try:
        pg_cursor.execute(query_film_works(film_work_ids))

        while True:
            records = pg_cursor.fetchmany(batch_size)

            if not records:
                break

            yield records
    except Exception as err:
        log.error(f'{datetime.now()} Failed while extracting film_works data.'
                  f'\n{err}\n\n')
        raise
