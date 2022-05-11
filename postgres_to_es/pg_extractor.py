import logging
from datetime import datetime
from typing import Optional, Any

import psycopg2  # type: ignore
from psycopg2.extras import RealDictCursor, RealDictRow  # type: ignore

from backoff import backoff
from config import dsn

log = logging.getLogger('Postgres')


def to_query_str(l: list[str]) -> str:
    """
    Returns a string for the inner part of an SQL-tuple.

    > to_query_str(['1', '2', '3'])
    "'1', '2', '3'"  # in SQL it will become `('1', '2', '3')`
    """
    return ', '.join("'" + i + "'" for i in l)


def only_first_els(ts: list[tuple]) -> list[Any]:
    """
    Returns a list with first elements of tuples from the given list of tuples.

    > only_first_els([(1, "a"), (2, "b"), (3, "c")])
    [1, 2, 3]
    """
    return [el[0] for el in ts]


def query_table_ids(table_name: str, from_modified: str, limit=100):
    return f"""
    SELECT id, modified
    FROM content.{table_name}
    WHERE modified > '{from_modified}'
    ORDER BY modified
    LIMIT {limit}; 
    """


def query_film_work_ids(table_name: str, ids: list[str], limit=100) -> str:
    return f"""
    SELECT fw.id, fw.modified
    FROM content.film_work fw
    LEFT JOIN content.{table_name}_film_work tfw ON tfw.film_work_id = fw.id
    WHERE tfw.{table_name}_id IN ({to_query_str(ids)})
    ORDER BY fw.modified
    LIMIT {limit};
    """


def query_film_works(film_work_ids: list[str]) -> str:
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
def connect_pg():
    try:
        conn = psycopg2.connect(**dsn, cursor_factory=RealDictCursor)
        log.info(f'{datetime.now()} Successfully connected to Postgres.')
        return conn.cursor()
    except Exception as err:
        log.error(f'{datetime.now()} Postgres connection failed.\n{err}\n\n')
        raise


@backoff()
def extract_from_pg(pg_cursor: RealDictCursor,
                    table_name: str,
                    last_modified: str,
                    batch: int = 3,
                    query_limit: int = 10):
    try:
        # Collect modified person/genre/film_work ids
        q = query_table_ids(table_name, last_modified, query_limit)
        pg_cursor.execute(q)
        records = pg_cursor.fetchall()
        table_ids = [t['id'] for t in records]

        next_last_modified = records[-1].get('modified') if table_ids \
            else last_modified

        # Collect film_work ids
        if table_name == 'film_work':
            film_work_ids = table_ids
        elif table_ids:
            pg_cursor.execute(query_film_work_ids(table_name, table_ids))
            film_work_ids = [t['id'] for t in pg_cursor.fetchall()]
        else:
            film_work_ids = []

        # Collect all film_work data
        # TODO: use batch
        if film_work_ids:
            pg_cursor.execute(query_film_works(film_work_ids))
            film_works = pg_cursor.fetchall()
        else:
            film_works = []
        return film_works, next_last_modified
    except Exception as err:
        log.error(f'{datetime.now()} Failed while extracting data '
                  f'from Postgres.\n{err}\n\n')
        raise
