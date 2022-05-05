import logging
from datetime import datetime
from backoff import backoff

log = logging.getLogger('Postgres')

extract_query = """
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
WHERE fw.modified > %s
GROUP BY fw.id
ORDER BY fw.modified
LIMIT %s;
"""


@backoff()
def extract_from_pg(pg_cursor, last_modified, batch=3, query_limit=10) -> list:
    data = []

    try:
        pg_cursor.execute(extract_query, (last_modified, query_limit))

        while True:
            records = pg_cursor.fetchmany(batch)

            if not records:
                break

            for row in records:
                # TODO: probably use generator? https://stackoverflow.com/a/39039564
                data.append(row)

    except Exception as err:
        log.error(f'{datetime.now()} Failed while extracting data from Postgres.\n{err}\n\n')
        raise

    return data
