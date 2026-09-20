# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""e2e against the original Apache drivers:
- pyarrow FlightClient (C++/gRPC C-core) — transport and data plane;
- adbc-driver-flightsql (Go/gRPC-Go) — the full Flight SQL flow;
- the Go harness (github.com/apache/arrow-go) — a separate process
  (test_go_driver.py).

The suite seeds its own database over the wire (DDL/DML via DoPut) and drops
it on teardown; nothing here depends on the rest of the test data.
"""

import os
import subprocess
import sys

import pyarrow
import pyarrow.flight as fl
import pytest
from google.protobuf import any_pb2

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
GEN = os.path.join(HERE, "gen")
if not os.path.exists(os.path.join(GEN, "Flight_pb2.py")):
    os.makedirs(GEN, exist_ok=True)
    subprocess.check_call(
        [sys.executable, "-m", "grpc_tools.protoc",
         f"-I{os.path.join(ROOT, 'frontend/flight_sql/format')}",
         f"--python_out={GEN}",
         os.path.join(ROOT, "frontend/flight_sql/format/Flight.proto"),
         os.path.join(ROOT, "frontend/flight_sql/format/FlightSql.proto")])
sys.path.insert(0, GEN)
import FlightSql_pb2 as sqlpb  # noqa: E402

DB = "flight_e2e"
CATALOG = DB
PH = "$1"

adbc_dbapi = pytest.importorskip("adbc_driver_flightsql.dbapi")


def sql(query_template):
    """Substitute the DB prefix: sql('SELECT * FROM {db}.people')."""
    return query_template.format(db=DB)


def command_any(cmd):
    any_msg = any_pb2.Any()
    any_msg.Pack(cmd)
    return any_msg.SerializeToString()


def run_select(client, sql_text):
    cmd = sqlpb.CommandStatementQuery()
    cmd.query = sql_text
    info = client.get_flight_info(fl.FlightDescriptor.for_command(command_any(cmd)))
    return client.do_get(info.endpoints[0].ticket).read_all()


@pytest.fixture(scope="module")
def seeded(server_uri):
    """The suite's own database: numbers (16 rows, a NULL) + people (5 rows)."""
    client = fl.connect(server_uri)

    def do_update(sql_text):
        cmd = sqlpb.CommandStatementUpdate()
        cmd.query = sql_text
        writer, reader = client.do_put(
            fl.FlightDescriptor.for_command(command_any(cmd)), pyarrow.schema([]))
        writer.close()
        # A driver that surfaces a server error raises here (writer.close);
        # pyarrow's FlightMetadataReader.read() may answer None for a PutResult
        # this stack does not surface, so the record count is not read here —
        # the ADBC cases below assert the counts where the driver reports them.
        reader.read()

    try:
        do_update(f"DROP DATABASE IF EXISTS {DB}")
    except Exception:
        pass  # nothing left over
    do_update(f"CREATE DATABASE {DB}")
    for ddl in [
        f"CREATE TABLE {DB}.numbers (id BIGINT, name STRING, score DOUBLE)",
        f"CREATE TABLE {DB}.people (person_id BIGINT, full_name STRING, age INT, active BOOLEAN)",
    ]:
        do_update(ddl)

    numbers = ", ".join(
        f"({i}, {'NULL' if i == 3 else chr(39) + f'row-{i}' + chr(39)}, {i}.5)" for i in range(16))
    do_update(f"INSERT INTO {DB}.numbers (id, name, score) VALUES {numbers}")
    people = ", ".join([
        "(100, 'Alice', 21, true)",
        "(101, 'Bob', NULL, false)",
        "(102, 'Carol', 31, true)",
        "(103, 'Dave', 40, NULL)",
        "(104, 'Frank', 52, true)",
    ])
    do_update(f"INSERT INTO {DB}.people (person_id, full_name, age, active) VALUES {people}")
    yield client
    try:
        do_update(f"DROP DATABASE {DB}")
    except Exception as e:  # noqa: BLE001
        print(f"cleanup: DROP DATABASE {DB} failed: {e}")


# ---------------------------------------------------------------- pyarrow ----

class TestPyArrowFlightClient:
    def test_handshake_anonymous(self, server_uri):
        client = fl.connect(server_uri)
        pair = client.authenticate_basic_token(b"", b"")
        assert pair is not None

    def test_list_actions(self, server_uri):
        client = fl.connect(server_uri)
        actions = {t for t, _ in client.list_actions()}
        assert "CreatePreparedStatement" in actions
        assert "ClosePreparedStatement" in actions

    def test_select_and_do_get(self, seeded):
        table = run_select(seeded, sql("SELECT * FROM {db}.numbers"))
        assert table.num_rows == 16
        names = table.column("name").to_pylist()
        assert names[3] is None
        assert names[0] == "row-0"

    def test_limit_slicing(self, seeded):
        table = run_select(seeded, sql("SELECT * FROM {db}.numbers LIMIT 5"))
        assert table.num_rows == 5
        assert table.column("name").to_pylist() == ["row-0", "row-1", "row-2", None, "row-4"]

    def test_get_schema(self, seeded):
        cmd = sqlpb.CommandStatementQuery()
        cmd.query = sql("SELECT * FROM {db}.people")
        result = seeded.get_schema(fl.FlightDescriptor.for_command(command_any(cmd)))
        assert "person_id" in result.schema.names
        assert "full_name" in result.schema.names

    def test_unknown_ticket_not_found(self, server_uri):
        client = fl.connect(server_uri)
        with pytest.raises(KeyError):
            list(client.do_get(fl.Ticket(b"garbage-ticket")))

    def test_bad_command_rejected(self, server_uri):
        client = fl.connect(server_uri)
        with pytest.raises(pyarrow.lib.ArrowInvalid):
            client.get_flight_info(fl.FlightDescriptor.for_command(b"not-an-any"))


# ------------------------------------------------------------------- ADBC ----

class TestADBCDriver:
    def test_select(self, server_uri):
        with adbc_dbapi.connect(server_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(sql("SELECT * FROM {db}.numbers"))
                table = cur.fetch_arrow_table()
                assert table.num_rows == 16

    def test_select_where_literal(self, server_uri):
        with adbc_dbapi.connect(server_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(sql("SELECT * FROM {db}.numbers WHERE id >= 13"))
                assert cur.fetch_arrow_table().num_rows == 3

    def test_insert_update(self, server_uri):
        with adbc_dbapi.connect(server_uri) as conn:
            cur = conn.cursor()
            stmt = cur.adbc_statement
            stmt.set_sql_query(
                sql("INSERT INTO {db}.people (person_id, full_name, age, active) ") +
                "VALUES (900001, 'E2E', 42, true)")
            assert stmt.execute_update() == 1
            cur.execute(sql("SELECT * FROM {db}.people WHERE person_id = 900001"))
            assert cur.fetch_arrow_table().num_rows == 1
            cur.close()

    def test_get_objects(self, server_uri):
        # The metadata mirrors the REGISTERED backends (the catalog store holds
        # their mirrored tables): with backends up their databases answer as
        # catalogs, without any the root catalog does. The test runs in both
        # environments and asserts the answer's shape, not the deployment.
        with adbc_dbapi.connect(server_uri) as conn:
            objects = conn.adbc_get_objects(depth="all").read_all()
            assert objects.num_rows >= 1
            catalogs = objects.column("catalog_name").to_pylist()
            assert all(isinstance(c, str) for c in catalogs)

    def test_get_info(self, server_uri):
        with adbc_dbapi.connect(server_uri) as conn:
            info = conn.adbc_get_info()
            assert len(list(info)) > 0


class TestADBCPrepared:
    def test_int_parameter(self, server_uri):
        with adbc_dbapi.connect(server_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {DB}.people WHERE person_id = {PH}",
                    parameters=pyarrow.RecordBatch.from_arrays(
                        [pyarrow.array([100], type=pyarrow.int64())], names=["0"]))
                table = cur.fetch_arrow_table()
                assert table.num_rows == 1
                assert table.column("full_name").to_pylist() == ["Alice"]

    def test_string_parameter(self, server_uri):
        with adbc_dbapi.connect(server_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {DB}.people WHERE full_name = {PH}",
                    parameters=pyarrow.RecordBatch.from_arrays(
                        [pyarrow.array(["Carol"], type=pyarrow.string())], names=["0"]))
                table = cur.fetch_arrow_table()
                assert table.column("person_id").to_pylist() == [102]

    def test_parameterized_update(self, server_uri):
        # A parameterized UPDATE through the cursor (the driver's bind path);
        # the effect is read back rather than the driver's affected count,
        # which pyarrow do_put does not surface (see do_update above).
        with adbc_dbapi.connect(server_uri) as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE {DB}.numbers SET score = 42.0 WHERE id = {PH}",
                            parameters=pyarrow.RecordBatch.from_arrays(
                                [pyarrow.array([2], type=pyarrow.int64())], names=["0"]))
                cur.execute(sql("SELECT score FROM {db}.numbers WHERE id = 2"))
                assert cur.fetch_arrow_table().column("score").to_pylist() == [42.0]

    def test_lowlevel_prepare_lifecycle(self, server_uri):
        with adbc_dbapi.connect(server_uri) as conn:
            with conn.cursor() as cur:
                stmt = cur.adbc_statement
                stmt.set_sql_query(f"SELECT * FROM {DB}.people WHERE age > {PH}")
                stmt.prepare()
                stmt.bind(pyarrow.RecordBatch.from_arrays(
                    [pyarrow.array([25], type=pyarrow.int64())], names=["0"]))
                stream, _ = stmt.execute_query()
                table = pyarrow.RecordBatchReader.from_stream(stream).read_all()
                # the seeded 3 rows over 25 (Carol/Dave/Frank); other session
                # tests may have added their own people
                assert table.num_rows >= 3
                assert "Alice" not in table.column("full_name").to_pylist()
