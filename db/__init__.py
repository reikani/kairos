import os
from contextlib import contextmanager

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

SERVER = os.getenv("SERVER", "mini")

_pool = ConnectionPool(
    conninfo=f"postgresql://postgres:postgres@{SERVER}:5432/kairos",
    min_size=1,
    max_size=int(os.getenv("PG_POOL_MAX", "10")),
    kwargs={"row_factory": dict_row},
)


def _conn():
    with _pool.connection() as c:
        try:
            yield c
            c.commit()
        except Exception:
            c.rollback()
            raise


conn = contextmanager(_conn)
