# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 OtterStax

"""Real asyncpg coverage for engine-driven PostgreSQL parameter typing."""

import asyncio
import os

import asyncpg

from config import PG_PORT


OTTERSTAX = dict(host=os.environ.get("OTTERSTAX_HOST", "test-otterstax"),
                 port=int(os.environ.get("OTTERSTAX_PORT", str(PG_PORT))), user="testuser",
                 password="testpass", database="local")
BACKEND = dict(host=os.environ.get("BACKEND_HOST", "postgres1"),
               port=int(os.environ.get("BACKEND_PORT", "5432")), user="pguser",
               password="pgpassword", database="pgdb")
LOCAL_TABLE = f"prepared_async_cases_{os.getpid()}"
REMOTE_TABLE = "prepared_async_remote"


async def prepare(conn, sql, expected_oid, *args):
    statement = await conn.prepare(sql)
    parameters = statement.get_parameters()
    assert len(parameters) == len(args), (sql, parameters)
    assert parameters[0].oid == expected_oid, (sql, parameters[0])
    return await statement.fetch(*args)


async def main():
    backend = await asyncpg.connect(**BACKEND)
    conn = await asyncpg.connect(**OTTERSTAX)
    try:
        await backend.execute(f"DROP TABLE IF EXISTS public.{REMOTE_TABLE}")
        await backend.execute(
            f"CREATE TABLE public.{REMOTE_TABLE} (id integer, score integer, label text)"
        )
        await backend.executemany(
            f"INSERT INTO public.{REMOTE_TABLE} (id, score, label) VALUES ($1, $2, $3)",
            [(7, 41, "remote"), (8, 9, "other")],
        )

        await conn.execute(
            f"CREATE TABLE {LOCAL_TABLE} (id integer, score integer, label text)"
        )
        await conn.execute(
            f"INSERT INTO {LOCAL_TABLE} (id, score, label) VALUES "
            "(1, 41, 'local'), (2, 9, 'other')"
        )

        # Empty Parse OID list: the engine supplies int4 for the predicate.
        rows = await prepare(conn, f"SELECT id FROM {LOCAL_TABLE} WHERE id = $1", 23, 1)
        assert [r["id"] for r in rows] == [1]

        # Arithmetic, text, and LIMIT all use the engine's inferred type.
        rows = await prepare(conn, f"SELECT score + $1 AS score FROM {LOCAL_TABLE} WHERE id = 1", 23, 1)
        assert rows[0]["score"] == 42
        rows = await prepare(conn, f"SELECT id FROM {LOCAL_TABLE} WHERE label = $1", 25, "local")
        assert [r["id"] for r in rows] == [1]
        # A LIMIT takes a bigint, as PostgreSQL types the count.
        rows = await prepare(conn, f"SELECT id FROM {LOCAL_TABLE} ORDER BY id LIMIT $1", 20, 1)
        assert [r["id"] for r in rows] == [1]

        # A table created after startup exercises missed-table registration on a remote backend.
        rows = await prepare(conn, f"SELECT id FROM products.public.{REMOTE_TABLE} WHERE id = $1", 23, 7)
        assert [r["id"] for r in rows] == [7]

        try:
            await conn.prepare("SELECT $1")
        except asyncpg.IndeterminateDatatypeError as error:
            assert error.sqlstate == "42P18", error
        else:
            raise AssertionError("SELECT $1 must be refused with SQLSTATE 42P18")
    finally:
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {LOCAL_TABLE}")
        except asyncpg.PostgresError:
            pass
        await conn.close()
        await backend.execute(f"DROP TABLE IF EXISTS public.{REMOTE_TABLE}")
        await backend.close()


if __name__ == "__main__":
    asyncio.run(main())
