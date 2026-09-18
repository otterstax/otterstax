# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""PostgreSQL extended-protocol contract of the PG frontend, on the raw wire.

Client libraries hide the messages this file is about: psycopg's server-side
cursors are DECLARE/FETCH statements, not Execute row limits, and every driver
re-prepares on its own terms. So the test speaks the protocol itself over a
socket (StartupMessage, Parse, Bind, Describe, Execute, Sync) and asserts on
the exact message sequence the server answers with:

  * Execute with a row limit smaller than the result: the rows up to the limit,
    then PortalSuspended; the next Execute on the same portal continues where
    the previous one stopped and the last one ends with CommandComplete.
  * Describe of a statement: ParameterDescription carrying the parameter OIDs
    the Parse declared, in front of RowDescription / NoData. A SELECT is always
    described, never answered NoData — before Bind the row shape is the
    statement's projection resolved against the columns the engine reports for
    the relations it reads, so a named column carries the type its values will
    arrive under and `SELECT *` goes out expanded into the table's own columns.
    What that computation cannot answer — the width of a JOIN whose same-named
    columns it merges — is what the Execute check below is for.
  * Describe of a portal: a portal is a bound statement, so a statement that
    answers rows is RUN and described by the result it actually answers — the
    real column types — and its Execute streams that stored result rather than
    running anything again. A DML portal is not run: it answers NoData, and the
    Execute is what writes, exactly once.
  * Execute of a statement described before Bind whose executed result has
    another shape (a different column count, or a column under another type
    oid): ErrorResponse 0A000 "cached plan must not change result type", as
    PostgreSQL answers, and not one row under the shape the client holds.
  * A named statement executed twice, and again after a failed execution: the
    Worker keeps a prepared statement for one execution only, so the frontend
    prepares it again under the hood; a second Execute must never surface the
    Worker's "must be re-prepared" verdict.
  * An error inside an extended-query pipeline (Parse, Bind, Describe,
    Execute): ErrorResponse alone, the rest of the pipeline is discarded, and
    the Sync answers the one ReadyForQuery — never two. A failed simple Query
    keeps ErrorResponse followed by ReadyForQuery at once.
  * Bind with a text literal the parameter type cannot hold ('12abc' for an
    int8): ErrorResponse with 22P02, and no BindComplete before it.
  * Bind asking for binary result columns: RowDescription reports format code
    1 for every column.
  * A `hugeint` result column is carried in text: it is described as NUMERIC
    (oid 1700) — PostgreSQL's own fixed-point type, variable length, no declared
    typmod — from a simple Query, from Describe and from an Execute the client
    never described. In **binary** it is refused, like every NUMERIC: binary
    NUMERIC is PostgreSQL's backend-internal digit-group encoding, which this
    frontend does not write, and a DataRow states each field's length before its
    bytes, so a guessed encoding would desynchronise the row. The refusal is
    ErrorResponse 0A000 naming the column, its type and the format.
  * A result column the PG wire cannot encode at all — a `uhugeint` column, for
    which the frontend maps no type — is ErrorResponse 0A000 naming the column,
    its type and the result format, from a simple Query, from Describe and from
    Execute, and the same connection keeps serving. Before this path existed the
    encoder threw and the connection was dropped.
  * A SELECT whose only column is one of those: the carried one is a
    RowDescription plus 'SELECT 0', the refused one is the ErrorResponse —
    never a bare CommandComplete. A column-less payload is what every frontend
    reports as a DML, so neither shape may degrade into "SELECT 0" with no
    RowDescription, on the simple Query and on the Execute of a portal the
    client never described alike.
  * A named statement parsed but never executed, then Terminate or a socket
    drop: the connection's teardown releases it on the Worker
    (Scheduler::close_statement) and the server keeps accepting and serving
    connections. The release is not visible on the wire — the case proves the
    teardown path completes (no crash, no wedged pool slot), not the Worker-
    side erasure.
  * A message truncated inside its fields after startup (an Execute cut
    before its row limit, a Query without its terminator): ErrorResponse with
    severity ERROR and SQLSTATE 08P01, like PostgreSQL — inside a pipeline
    the rest is discarded up to the Sync, which answers the one ReadyForQuery;
    a simple Query gets ErrorResponse + ReadyForQuery at once — and the same
    connection keeps serving. Every message is length-prefixed and the server
    consumes exactly the declared length, so the next message is framed right.
  * The ReadyForQuery status byte follows the transaction block, as in
    PostgreSQL: 'I' outside one (an error there fails nothing), 'T' after
    BEGIN, 'E' once an error inside the block failed it — on the Sync of a
    pipeline and on a simple Query alike — and 'I' again after ROLLBACK or
    after a COMMIT, which rolls a failed block back.

Everything runs against otterbrix-internal tables, so no backend is involved.
"""

import argparse
import socket
import struct
import sys

from config import get_host, PG_PORT

DB = "pg_ext_proto"
TABLE = "rows"
ROWS = 5

WIDE_TABLE = "wide"    # (id INT, big hugeint)  — carried as NUMERIC in text
UWIDE_TABLE = "uwide"  # (id INT, big uhugeint) — no wire type at all

# Two tables with the same column names: a prepare merges a JOIN of them by
# name and describes two columns, the engine answers all four.
JOIN_LEFT = "people"
JOIN_RIGHT = "tags"

OID_INT4 = 23
OID_INT8 = 20
OID_TEXT = 25
OID_NUMERIC = 1700
SQLSTATE_INVALID_TEXT_REPRESENTATION = "22P02"
SQLSTATE_FEATURE_NOT_SUPPORTED = "0A000"
SQLSTATE_PROTOCOL_VIOLATION = "08P01"

# Message type bytes the assertions name.
PARSE_COMPLETE = b"1"
BIND_COMPLETE = b"2"
PARAMETER_DESCRIPTION = b"t"
ROW_DESCRIPTION = b"T"
NO_DATA = b"n"
DATA_ROW = b"D"
PORTAL_SUSPENDED = b"s"
COMMAND_COMPLETE = b"C"
ERROR_RESPONSE = b"E"
READY_FOR_QUERY = b"Z"


def _cstr(value):
    return value.encode() + b"\x00"


class PgWire:
    """The handful of frontend messages the test needs, framed by hand."""

    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=30)

    def close(self):
        self.sock.close()

    # ── sending ────────────────────────────────────────────────────────────
    def _send(self, kind, payload):
        self.sock.sendall(kind + struct.pack("!i", len(payload) + 4) + payload)

    def startup(self, user, database):
        payload = struct.pack("!i", 196608) + _cstr("user") + _cstr(user) + _cstr("database") + _cstr(database) + b"\x00"
        self.sock.sendall(struct.pack("!i", len(payload) + 4) + payload)
        return self.read_until_ready()

    def query(self, sql):
        self._send(b"Q", _cstr(sql))
        return self.read_until_ready()

    def parse(self, statement, sql, param_oids=()):
        payload = _cstr(statement) + _cstr(sql) + struct.pack("!h", len(param_oids))
        for oid in param_oids:
            payload += struct.pack("!i", oid)
        self._send(b"P", payload)

    def bind(self, portal, statement, text_params=(), result_formats=()):
        payload = _cstr(portal) + _cstr(statement)
        payload += struct.pack("!h", 0)                     # every parameter in text format
        payload += struct.pack("!h", len(text_params))
        for value in text_params:
            raw = value.encode()
            payload += struct.pack("!i", len(raw)) + raw
        payload += struct.pack("!h", len(result_formats))
        for code in result_formats:
            payload += struct.pack("!h", code)
        self._send(b"B", payload)

    def describe(self, kind, name):
        self._send(b"D", kind.encode() + _cstr(name))

    def execute(self, portal, max_rows):
        self._send(b"E", _cstr(portal) + struct.pack("!i", max_rows))

    def send_raw(self, kind, payload):
        """A message with an arbitrary, possibly truncated payload; the length prefix is honest."""
        self._send(kind, payload)

    def sync(self):
        self._send(b"S", b"")

    def terminate(self):
        self._send(b"X", b"")

    # ── receiving ──────────────────────────────────────────────────────────
    def _recv_exact(self, size):
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise ConnectionError("server closed the connection")
            data += chunk
        return data

    def read_message(self):
        kind = self._recv_exact(1)
        (length,) = struct.unpack("!i", self._recv_exact(4))
        return kind, self._recv_exact(length - 4)

    def read_until_ready(self):
        """Every message up to and including ReadyForQuery, in order."""
        messages = []
        while True:
            kind, payload = self.read_message()
            messages.append((kind, payload))
            if kind == READY_FOR_QUERY:
                return messages


# ── payload decoders ───────────────────────────────────────────────────────
def parameter_oids(payload):
    (count,) = struct.unpack("!h", payload[:2])
    return list(struct.unpack("!" + "i" * count, payload[2:2 + 4 * count]))


def row_description_fields(payload):
    """[{name, type_oid, type_size, type_modifier, format}] of a RowDescription."""
    (count,) = struct.unpack("!h", payload[:2])
    offset = 2
    fields = []
    for _ in range(count):
        end = payload.index(b"\x00", offset)
        name = payload[offset:end].decode()
        offset = end + 1
        table_oid, column_attr, type_oid, type_size, type_modifier, fmt = \
            struct.unpack("!ihihih", payload[offset:offset + 18])
        offset += 18
        fields.append({"name": name, "type_oid": type_oid, "type_size": type_size,
                       "type_modifier": type_modifier, "format": fmt})
    return fields


def row_description_formats(payload):
    return [field["format"] for field in row_description_fields(payload)]


def data_row_texts(payload):
    (count,) = struct.unpack("!h", payload[:2])
    offset = 2
    values = []
    for _ in range(count):
        (length,) = struct.unpack("!i", payload[offset:offset + 4])
        offset += 4
        if length < 0:
            values.append(None)
        else:
            values.append(payload[offset:offset + length].decode())
            offset += length
    return values


def command_tag(payload):
    return payload.rstrip(b"\x00").decode()


def error_fields(payload):
    fields = {}
    for part in payload.split(b"\x00"):
        if part:
            fields[chr(part[0])] = part[1:].decode()
    return fields


def kinds(messages):
    return [kind for kind, _ in messages]


def first(messages, kind):
    for k, payload in messages:
        if k == kind:
            return payload
    raise AssertionError(f"no {kind!r} message in {kinds(messages)}")


class PgExtendedProtocolTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.wire = None
        self.database_created = False
        self.table_created = False
        self.wide_table_created = False
        self.uwide_table_created = False
        self.join_tables_created = []

    # ── helpers ────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def assert_no_error(self, messages, what):
        for kind, payload in messages:
            if kind == ERROR_RESPONSE:
                raise AssertionError(f"{what}: {error_fields(payload)}")

    def connect(self):
        wire = PgWire(self.host, PG_PORT)
        wire.startup("testuser", DB)
        return wire

    def setup(self):
        self.wire = self.connect()
        self.assert_no_error(self.wire.query(f"CREATE DATABASE {DB}"), "CREATE DATABASE")
        self.database_created = True
        self.assert_no_error(self.wire.query(f"CREATE TABLE {DB}.{TABLE} (id bigint, name string)"), "CREATE TABLE")
        self.table_created = True
        values = ", ".join(f"({i}, 'row_{i}')" for i in range(1, ROWS + 1))
        self.assert_no_error(self.wire.query(f"INSERT INTO {DB}.{TABLE} (id, name) VALUES {values}"), "INSERT")
        # Both tables stay empty: the column type alone is what is tested, and an
        # empty result keeps its columns. `hugeint` is carried as NUMERIC in text
        # and refused in binary; `uhugeint` has no wire type and is refused in
        # either format.
        self.assert_no_error(self.wire.query(f"CREATE TABLE {DB}.{WIDE_TABLE} (id INT, big hugeint)"),
                             "CREATE TABLE with a hugeint column")
        self.wide_table_created = True
        self.assert_no_error(self.wire.query(f"CREATE TABLE {DB}.{UWIDE_TABLE} (id INT, big uhugeint)"),
                             "CREATE TABLE with a uhugeint column")
        self.uwide_table_created = True
        # The JOIN pair: every column name is shared, so what a prepare can
        # compute before Bind merges the two schemas into two columns while the
        # engine answers four.
        for table in (JOIN_LEFT, JOIN_RIGHT):
            self.assert_no_error(self.wire.query(f"CREATE TABLE {DB}.{table} (id bigint, name string)"),
                                 f"CREATE TABLE {table}")
            self.join_tables_created.append(table)
            self.assert_no_error(
                self.wire.query(f"INSERT INTO {DB}.{table} (id, name) VALUES (1, '{table}_1'), (2, '{table}_2')"),
                f"INSERT into {table}")

    def cleanup(self):
        if self.wire is None:
            return
        try:
            if self.table_created:
                self.wire.query(f"DROP TABLE {DB}.{TABLE}")
            if self.wide_table_created:
                self.wire.query(f"DROP TABLE {DB}.{WIDE_TABLE}")
            if self.uwide_table_created:
                self.wire.query(f"DROP TABLE {DB}.{UWIDE_TABLE}")
            for table in self.join_tables_created:
                self.wire.query(f"DROP TABLE {DB}.{table}")
            if self.database_created:
                self.wire.query(f"DROP DATABASE {DB}")
        finally:
            self.wire.close()

    def read_failed_pipeline(self, wire):
        """Messages of an extended-protocol pipeline that ended in an error.

        The frontend answers the ErrorResponse alone, discards the rest of the
        pipeline and answers its Sync with the single ReadyForQuery: exactly
        one ReadyForQuery, and it is the last message — nothing may trail it,
        or the next exchange would start on a dirty socket.
        """
        messages = wire.read_until_ready()
        self.assert_equal(kinds(messages).count(READY_FOR_QUERY), 1, "one ReadyForQuery per failed pipeline")
        self.assert_no_trailing_message(wire, "a failed pipeline")
        return messages

    def assert_no_trailing_message(self, wire, what):
        """The server sent nothing beyond the ReadyForQuery just read."""
        wire.sock.settimeout(1)
        try:
            kind, _ = wire.read_message()
        except socket.timeout:
            return
        finally:
            wire.sock.settimeout(30)
        raise AssertionError(f"{what}: a stale {kind!r} message follows the ReadyForQuery")

    def assert_unsupported_column_error(self, messages, what, encoding="text", type_name="UHUGEINT"):
        """The refusal of a column the wire cannot encode, with everything it must name.

        The frontend, not the engine, is what refuses: the column reaches it
        typed, and the message names the column, its logical type, the wire and
        the result format that has no encoder for it — `encoding` is the format
        the route asked for (text unless the Bind requested binary results).
        """
        fields = error_fields(first(messages, ERROR_RESPONSE))
        self.assert_equal(fields.get("C"), SQLSTATE_FEATURE_NOT_SUPPORTED, f"{what}: sqlstate")
        self.assert_equal(fields.get("S"), "ERROR", f"{what}: an unencodable column is an ERROR, not FATAL")
        message = fields.get("M", "")
        assert "column 'big'" in message, f"{what}: the message must name the column: {message}"
        assert type_name in message, f"{what}: the message must name the type: {message}"
        assert "PostgreSQL wire" in message, f"{what}: the message must name the wire: {message}"
        assert f"cannot encode in {encoding} format" in message, \
            f"{what}: the message must name the {encoding} result format: {message}"
        return message

    def assert_hugeint_field(self, field, what):
        """The RowDescription field a hugeint goes out under: NUMERIC, variable length, no typmod."""
        self.assert_equal(field["name"], "big", f"{what}: the column keeps its name")
        self.assert_equal(field["type_oid"], OID_NUMERIC,
                          f"{what}: a 128-bit integer rides PostgreSQL's fixed-point type")
        self.assert_equal(field["type_size"], -1, f"{what}: NUMERIC is variable length")
        self.assert_equal(field["type_modifier"], -1, f"{what}: no declared typmod, as PG sends for a bare numeric")
        self.assert_equal(field["format"], 0, f"{what}: the value travels in text")

    # ── cases ──────────────────────────────────────────────────────────────
    def test_execute_row_limit_suspends_the_portal(self):
        """Execute with max_rows < result: rows, PortalSuspended, then the rest."""
        w = self.wire
        w.parse("", f"SELECT id, name FROM {DB}.{TABLE} ORDER BY id")
        w.bind("", "")
        w.describe("P", "")
        w.execute("", 2)
        w.execute("", 2)
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "portal cursor")

        seen = kinds(messages)
        expected = [PARSE_COMPLETE, BIND_COMPLETE, ROW_DESCRIPTION,
                    DATA_ROW, DATA_ROW, PORTAL_SUSPENDED,
                    DATA_ROW, DATA_ROW, PORTAL_SUSPENDED,
                    DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY]
        self.assert_equal(seen, expected, "message sequence of a row-limited portal")

        ids = [data_row_texts(p)[0] for k, p in messages if k == DATA_ROW]
        self.assert_equal(ids, [str(i) for i in range(1, ROWS + 1)], "the three Executes cover every row once")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "SELECT 1",
                          "CommandComplete counts the rows of the last Execute only")
        print("  Execute(2) x2 + Execute(0): 2 + 2 + 1 rows, PortalSuspended twice ✓")

    def test_describe_statement_reports_parameter_oids(self):
        """Describe('S') answers ParameterDescription with the declared OIDs."""
        w = self.wire
        w.parse("described", f"SELECT id FROM {DB}.{TABLE} WHERE id = $1", [OID_INT8])
        w.describe("S", "described")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe statement")

        seen = kinds(messages)
        self.assert_equal(seen[:2], [PARSE_COMPLETE, PARAMETER_DESCRIPTION], "ParameterDescription follows ParseComplete")
        self.assert_equal(parameter_oids(first(messages, PARAMETER_DESCRIPTION)), [OID_INT8], "declared parameter OIDs")
        # A SELECT is described, never declared row-less: before Bind the row
        # shape is the statement's projection resolved against the columns the
        # engine reports for its own table, so the column keeps its name AND
        # carries the type its values arrive under — the placeholder sits in the
        # WHERE and changes neither.
        self.assert_equal(seen[2], ROW_DESCRIPTION, "a parameterized SELECT is described, not answered NoData")
        fields = row_description_fields(first(messages, ROW_DESCRIPTION))
        self.assert_equal([f["name"] for f in fields], ["id"], "the projected column keeps its name")
        self.assert_equal(fields[0]["type_oid"], OID_INT8, "and is typed from the engine's own column")
        print("  ParameterDescription [int8] + RowDescription [id int8] ✓")

    def test_describe_statement_expands_a_star_before_bind(self):
        """Describe('S') of `SELECT * ... WHERE id = $1`: the table's columns, and the Execute streams them."""
        w = self.wire
        w.parse("star", f"SELECT * FROM {DB}.{TABLE} WHERE id = $1", [OID_INT8])
        w.describe("S", "star")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe(statement) of a parameterized SELECT *")

        # The star names no column on the plan — the transformer leaves it a
        # passthrough — so the columns come from the engine's own catalog, the
        # way PostgreSQL expands the star at parse time. Before this, the
        # description carried no fields at all and the Execute below was refused
        # on every such statement.
        fields = row_description_fields(first(messages, ROW_DESCRIPTION))
        self.assert_equal([f["name"] for f in fields], ["id", "name"], "the star is expanded into the table's columns")
        self.assert_equal([f["type_oid"] for f in fields], [OID_INT8, OID_TEXT], "under the engine's own types")

        # Bind and Execute WITHOUT Describe('P') — the shape the clients that
        # live by the statement's description use (Npgsql prepared, asyncpg,
        # tokio-postgres, lib/pq). The rows must arrive under the description
        # the client already holds: no second RowDescription, no 0A000.
        w.bind("", "star", ["2"])
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Execute of the described SELECT *")
        self.assert_equal(kinds(messages), [BIND_COMPLETE, DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "the rows go out under the description already given")
        self.assert_equal(data_row_texts(first(messages, DATA_ROW)), ["2", "row_2"], "the described result is streamed")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "SELECT 1", "one row")
        print("  Describe(S) of SELECT * -> [id int8, name text], Execute streams the row ✓")

    def test_describe_statement_of_a_returning_dml_names_its_columns(self):
        """Describe('S') of an INSERT ... RETURNING: the projected columns, never NoData."""
        w = self.wire
        before = self.row_count()

        w.parse("ret", f"INSERT INTO {DB}.{TABLE} (id, name) VALUES ($1, $2) RETURNING id, name",
                [OID_INT8, OID_TEXT])
        w.describe("S", "ret")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe(statement) of an INSERT ... RETURNING")

        # A RETURNING list answers rows, so the statement is described by the
        # columns it projects — typed from the target relation, resolved at
        # prepare by a LIMIT 0 probe. NoData here would state that no rows are
        # coming: lib/pq takes that at its word and hands the caller a row of
        # zero values instead of an error.
        fields = row_description_fields(first(messages, ROW_DESCRIPTION))
        self.assert_equal([f["name"] for f in fields], ["id", "name"], "the RETURNING list is described")
        self.assert_equal([f["type_oid"] for f in fields], [OID_INT8, OID_TEXT], "under the target table's types")
        self.assert_equal(self.row_count(), before, "describing a DML writes nothing")

        # Bind and Execute WITHOUT Describe('P'): the rows must arrive under the
        # description already held, with no 0A000 and no second RowDescription.
        w.bind("", "ret", ["900", "nine"])
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Execute of the described INSERT ... RETURNING")
        self.assert_equal(kinds(messages), [BIND_COMPLETE, DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "the returned row goes out under the description already given")
        self.assert_equal(data_row_texts(first(messages, DATA_ROW)), ["900", "nine"], "the inserted row is returned")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "INSERT 0 1", "one row inserted")
        self.assert_equal(self.row_count(), before + 1, "the Execute is what writes, exactly once")

        # The table is shared with the cases that follow, and one of them checks
        # the count against ROWS: take the inserted row back out so this case
        # leaves the fixture as it found it.
        self.assert_no_error(self.wire.query(f"DELETE FROM {DB}.{TABLE} WHERE id = 900"), "undo the inserted row")
        self.assert_equal(self.row_count(), before, "the fixture is left as it was found")

        # The control: the same statement without RETURNING answers no rows, and
        # NoData is the truthful answer there.
        w.parse("noret", f"INSERT INTO {DB}.{TABLE} (id, name) VALUES ($1, $2)", [OID_INT8, OID_TEXT])
        w.describe("S", "noret")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe(statement) of an INSERT without RETURNING")
        self.assert_equal([k for k, _ in messages].count(ROW_DESCRIPTION), 0,
                          "a DML without RETURNING is still NoData")
        print("  Describe(S) of INSERT ... RETURNING -> [id int8, name text], Execute writes once ✓")

    def row_count(self):
        """The table's row count, read on the shared connection."""
        messages = self.wire.query(f"SELECT count(*) FROM {DB}.{TABLE}")
        self.assert_no_error(messages, "count(*)")
        return int([data_row_texts(p)[0] for k, p in messages if k == DATA_ROW][0])

    def test_describe_portal_describes_the_executed_result(self):
        """Describe('P') of a bound SELECT: the real column types, and the Execute streams that result."""
        w = self.wire
        w.parse("pdesc", f"SELECT id, name FROM {DB}.{TABLE} WHERE id = $1", [OID_INT8])
        w.bind("p1", "pdesc", ["2"])
        w.describe("P", "p1")
        w.execute("p1", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe(portal) of a parameterized SELECT")

        self.assert_equal(kinds(messages),
                          [PARSE_COMPLETE, BIND_COMPLETE, ROW_DESCRIPTION, DATA_ROW, COMMAND_COMPLETE,
                           READY_FOR_QUERY],
                          "the portal is described once and its rows follow")
        fields = row_description_fields(first(messages, ROW_DESCRIPTION))
        self.assert_equal([f["name"] for f in fields], ["id", "name"], "both columns are described")
        # Every parameter is bound in a portal, so this description is the
        # executed result's own — not the untyped guess Describe('S') carries.
        self.assert_equal([f["type_oid"] for f in fields], [OID_INT8, OID_TEXT],
                          "a bound portal is described by the result it answers")
        self.assert_equal(data_row_texts(first(messages, DATA_ROW)), ["2", "row_2"],
                          "the described result is the one streamed")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "SELECT 1", "one row, run once")
        print("  Describe(portal) -> int8/text off the executed result ✓")

    def test_describe_portal_of_a_dml_does_not_execute_it(self):
        """Describe('P') of an INSERT answers NoData and writes nothing; its Execute writes exactly once."""
        w = self.wire
        before = self.row_count()

        w.parse("dml_desc", f"INSERT INTO {DB}.{TABLE} (id, name) VALUES ($1, $2)", [OID_INT8, OID_TEXT])
        w.bind("dp", "dml_desc", ["77", "row_77"])
        w.describe("P", "dp")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe(portal) of an INSERT")
        self.assert_equal(kinds(messages), [PARSE_COMPLETE, BIND_COMPLETE, NO_DATA, READY_FOR_QUERY],
                          "a DML portal is described without being run")
        self.assert_equal(self.row_count(), before, "describing a DML portal must not write")

        w.execute("dp", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Execute of the described INSERT portal")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "INSERT 0 1", "the INSERT ran")
        self.assert_equal(self.row_count(), before + 1, "exactly one row was written, by the Execute alone")

        self.assert_no_error(self.wire.query(f"DELETE FROM {DB}.{TABLE} WHERE id = 77"), "cleanup of the written row")
        print("  Describe(portal) of a DML: NoData, nothing written, Execute writes once ✓")

    def test_execute_refuses_a_result_shape_the_describe_did_not_promise(self):
        """A shape described before Bind that the result does not match: 0A000, never rows of another shape."""
        w = self.wire

        # A JOIN of two tables sharing every column name: what a prepare can
        # compute before Bind merges the two schemas BY NAME and describes two
        # columns, while the engine answers all four. A client that decodes by
        # the description it holds would read the row short, so the Execute is
        # refused the way PostgreSQL refuses it.
        joined = (f"SELECT * FROM {DB}.{JOIN_LEFT} JOIN {DB}.{JOIN_RIGHT} "
                  f"ON {JOIN_LEFT}.id = {JOIN_RIGHT}.id WHERE {JOIN_LEFT}.id = $1")
        w.parse("guessed", joined, [OID_INT8])
        w.describe("S", "guessed")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe(statement) of a parameterized JOIN")
        self.assert_equal([f["name"] for f in row_description_fields(first(messages, ROW_DESCRIPTION))],
                          ["id", "name"], "the same-named columns of the two tables are merged into one description")

        w.bind("", "guessed", ["2"])
        w.execute("", 0)
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(kinds(messages), [BIND_COMPLETE, ERROR_RESPONSE, READY_FOR_QUERY],
                          "the Execute is refused and nothing is streamed under the stale shape")
        fields = error_fields(first(messages, ERROR_RESPONSE))
        self.assert_equal(fields.get("C"), SQLSTATE_FEATURE_NOT_SUPPORTED, "PostgreSQL's sqlstate for this")
        self.assert_equal(fields.get("M"), "cached plan must not change result type", "PostgreSQL's own wording")
        assert DATA_ROW not in kinds(messages), "no row may go out under a shape the client was not given"

        # The control: a statement the prepare could type exactly is described
        # before Bind and executed under that very description — and the Execute
        # carries no RowDescription, because the client already has it.
        w.parse("exact", f"SELECT id, name FROM {DB}.{TABLE} WHERE id = 2")
        w.describe("S", "exact")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe(statement) of a statement without parameters")
        self.assert_equal([f["type_oid"] for f in row_description_fields(first(messages, ROW_DESCRIPTION))],
                          [OID_INT8, OID_TEXT], "the engine types this one")
        w.bind("", "exact")
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Execute of the exactly described statement")
        self.assert_equal(kinds(messages), [BIND_COMPLETE, DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "a described statement streams its rows without a second RowDescription")
        self.assert_equal(data_row_texts(first(messages, DATA_ROW)), ["2", "row_2"], "the row")
        print("  Execute after a stale Describe -> 0A000 'cached plan must not change result type' ✓")

    def test_named_statement_executes_twice_and_after_a_failure(self):
        """A named statement survives its first execution and a failed one."""
        w = self.wire
        w.parse("twice", f"SELECT name FROM {DB}.{TABLE} WHERE id = $1", [OID_INT8])
        w.sync()
        self.assert_no_error(w.read_until_ready(), "Parse named statement")

        for wanted in ("2", "4"):
            w.bind("", "twice", [wanted])
            w.execute("", 0)
            w.sync()
            messages = w.read_until_ready()
            self.assert_no_error(messages, f"Execute named statement with {wanted}")
            names = [data_row_texts(p)[0] for k, p in messages if k == DATA_ROW]
            self.assert_equal(names, [f"row_{wanted}"], "each Execute runs with its own parameter")

        # A statement whose execution fails on the engine: a text literal in the
        # bigint column is refused when the INSERT runs ("insert_node: can not
        # convert data column[0] type to table type"), not at Parse, whose
        # prepare of a DML statement does not touch the engine. The failure must
        # be the engine's verdict both times, never the Worker's re-prepare
        # demand.
        w.parse("failing", f"INSERT INTO {DB}.{TABLE} (id, name) VALUES ('not a number', 'row_x')")
        w.sync()
        self.assert_no_error(w.read_until_ready(), "Parse of an INSERT with a literal the column cannot hold")
        for attempt in (1, 2):
            w.bind("", "failing")
            w.execute("", 0)
            w.sync()
            messages = self.read_failed_pipeline(w)
            assert ERROR_RESPONSE in kinds(messages), f"attempt {attempt}: the unconvertible literal must be an error"
            message = error_fields(first(messages, ERROR_RESPONSE)).get("M", "")
            assert "re-prepared" not in message, f"attempt {attempt}: the re-prepare leaked to the client: {message}"
            assert COMMAND_COMPLETE not in kinds(messages), f"attempt {attempt}: nothing completes after the error"
        self.assert_equal([data_row_texts(p)[0] for k, p in self.wire.query(f"SELECT count(*) FROM {DB}.{TABLE}")
                           if k == DATA_ROW], [str(ROWS)], "the failed INSERTs wrote nothing")
        print("  named statement: two Executes, then two failing ones, no re-prepare leak ✓")

    def test_bind_rejects_a_literal_the_type_cannot_hold(self):
        """Bind '12abc' for an int8: ErrorResponse 22P02 and no BindComplete."""
        w = self.wire
        w.parse("typed", f"SELECT id FROM {DB}.{TABLE} WHERE id = $1", [OID_INT8])
        w.bind("", "typed", ["12abc"])
        w.execute("", 0)
        w.sync()
        messages = self.read_failed_pipeline(w)

        seen = kinds(messages)
        self.assert_equal(seen, [PARSE_COMPLETE, ERROR_RESPONSE, READY_FOR_QUERY],
                          "a failed Bind: the error alone, the Execute discarded, the Sync's ReadyForQuery")
        fields = error_fields(first(messages, ERROR_RESPONSE))
        self.assert_equal(fields.get("C"), SQLSTATE_INVALID_TEXT_REPRESENTATION, "sqlstate of a malformed literal")

        # The statement is intact: a well-formed literal binds and runs.
        w.bind("", "typed", ["3"])
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Bind after a failed Bind")
        self.assert_equal([data_row_texts(p)[0] for k, p in messages if k == DATA_ROW], ["3"], "rows of the retried Bind")
        print("  Bind '12abc' as int8 -> 22P02, no BindComplete ✓")

    def test_binary_result_format_is_reported_in_row_description(self):
        """Result format code 1 in Bind: RowDescription says 1 for every column."""
        w = self.wire
        w.parse("", f"SELECT id, name FROM {DB}.{TABLE} WHERE id = 1")
        w.bind("", "", result_formats=[1])
        w.describe("P", "")
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "binary result format")

        self.assert_equal(row_description_formats(first(messages, ROW_DESCRIPTION)), [1, 1],
                          "one result format code applies to every column")
        self.assert_equal(kinds(messages).count(DATA_ROW), 1, "the row is still delivered")
        (id_len,) = struct.unpack("!i", first(messages, DATA_ROW)[2:6])
        self.assert_equal(id_len, 8, "a binary int8 is eight bytes on the wire")
        print("  RowDescription format code 1 for binary results ✓")

    def test_hugeint_column_is_carried_as_numeric_in_text_and_refused_in_binary(self):
        """A hugeint column: NUMERIC in text on every route, ErrorResponse 0A000 in binary."""
        w = self.wire

        # Simple Query: the executed chunk carries the column as HUGEINT, which
        # goes out as NUMERIC. The table is empty, so the resultset is a row
        # shape and a count — no DataRow.
        messages = w.query(f"SELECT * FROM {DB}.{WIDE_TABLE}")
        self.assert_no_error(messages, "a simple query over a hugeint column")
        self.assert_equal(kinds(messages), [ROW_DESCRIPTION, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "a carried hugeint column is described and completed")
        fields = row_description_fields(first(messages, ROW_DESCRIPTION))
        self.assert_equal([f["name"] for f in fields], ["id", "big"], "both columns are described")
        self.assert_equal(fields[0]["type_oid"], OID_INT4, "the int column keeps its own type oid")
        self.assert_hugeint_field(fields[1], "simple Query")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "SELECT 0", "the table is empty")

        # Describe(statement): the prepared schema carries the column too, so the
        # row shape is answered rather than refused.
        w.parse("wide_s", f"SELECT big FROM {DB}.{WIDE_TABLE}")
        w.describe("S", "wide_s")
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Describe of a hugeint column")
        self.assert_equal(kinds(messages), [PARSE_COMPLETE, PARAMETER_DESCRIPTION, ROW_DESCRIPTION, READY_FOR_QUERY],
                          "Describe(statement) answers the row shape")
        self.assert_hugeint_field(row_description_fields(first(messages, ROW_DESCRIPTION))[0], "Describe(statement)")

        # Execute without Describe, text results: the row shape comes with the
        # Execute and the statement completes.
        w.parse("wide_t", f"SELECT * FROM {DB}.{WIDE_TABLE}")
        w.bind("", "wide_t")
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "Execute over a hugeint column in text")
        self.assert_equal(kinds(messages),
                          [PARSE_COMPLETE, BIND_COMPLETE, ROW_DESCRIPTION, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "a text Execute carries the column")
        self.assert_hugeint_field(row_description_fields(first(messages, ROW_DESCRIPTION))[1], "Execute (text)")

        # Execute with binary result format: refused. Binary NUMERIC is
        # PostgreSQL's internal digit-group encoding, which this frontend does
        # not write, and a DataRow states each field's length before its bytes.
        w.parse("wide_e", f"SELECT * FROM {DB}.{WIDE_TABLE}")
        w.bind("", "wide_e", result_formats=[1])
        w.execute("", 0)
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(kinds(messages), [PARSE_COMPLETE, BIND_COMPLETE, ERROR_RESPONSE, READY_FOR_QUERY],
                          "a refused binary Execute")
        assert DATA_ROW not in kinds(messages), "no row may be sent in a format the column has no encoder for"
        binary = self.assert_unsupported_column_error(messages, "Execute (binary)", encoding="binary",
                                                     type_name="HUGEINT")

        # The same connection keeps serving, simple and extended alike.
        messages = w.query("SELECT 1")
        self.assert_no_error(messages, "SELECT 1 after the binary refusal")
        self.assert_equal(kinds(messages), [ROW_DESCRIPTION, DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "SELECT 1 on the same connection")
        print(f"  hugeint -> NUMERIC in text, binary refused: '{binary}' ✓")

    def test_unsupported_column_type_is_an_error_not_a_disconnect(self):
        """A uhugeint column: ErrorResponse 0A000 naming it, on every path; the connection lives on."""
        w = self.wire

        # Simple Query: the executed chunk carries the column as UHUGEINT, which
        # the frontend maps to no type at all. The simple protocol has no Sync:
        # ErrorResponse and ReadyForQuery together.
        messages = w.query(f"SELECT * FROM {DB}.{UWIDE_TABLE}")
        self.assert_equal(kinds(messages), [ERROR_RESPONSE, READY_FOR_QUERY], "a refused simple query")
        self.assert_no_trailing_message(w, "a refused simple query")
        simple = self.assert_unsupported_column_error(messages, "simple Query", encoding="text")

        # Describe(statement): the prepared schema carries the column as well.
        w.parse("uwide_s", f"SELECT big FROM {DB}.{UWIDE_TABLE}")
        w.describe("S", "uwide_s")
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(kinds(messages), [PARSE_COMPLETE, ERROR_RESPONSE, READY_FOR_QUERY], "a refused Describe")
        assert ROW_DESCRIPTION not in kinds(messages), "no RowDescription may describe an unencodable column"
        self.assert_unsupported_column_error(messages, "Describe(statement)", encoding="text")

        # Execute without Describe: refused at Execute, in text as in binary.
        w.parse("uwide_e", f"SELECT * FROM {DB}.{UWIDE_TABLE}")
        w.bind("", "uwide_e")
        w.execute("", 0)
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(kinds(messages), [PARSE_COMPLETE, BIND_COMPLETE, ERROR_RESPONSE, READY_FOR_QUERY],
                          "a refused Execute")
        assert DATA_ROW not in kinds(messages), "no row may be sent for an unencodable column"
        self.assert_unsupported_column_error(messages, "Execute", encoding="text")

        # The same connection keeps serving, simple and extended alike.
        messages = w.query("SELECT 1")
        self.assert_no_error(messages, "SELECT 1 after the refusals")
        self.assert_equal(kinds(messages), [ROW_DESCRIPTION, DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "SELECT 1 on the same connection")
        w.parse("", f"SELECT id FROM {DB}.{TABLE} ORDER BY id")
        w.bind("", "")
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "extended protocol after the refusals")
        self.assert_equal(kinds(messages).count(DATA_ROW), ROWS, "the plain table still streams")
        print(f"  uhugeint column -> 0A000 '{simple}', connection alive ✓")

    def test_select_of_a_single_column_is_never_a_bare_command_complete(self):
        """SELECT big alone: RowDescription + 'SELECT 0' when carried, ErrorResponse when not.

        The whole result is one column. Zero result columns is exactly how a
        frontend recognises a DML, and the hazard of this shape is that it
        degrades into that: a CommandComplete with no RowDescription, which a
        client reads as a statement that ran fine. The carried column must be
        described and the unmapped one refused, identically on the simple route
        and on an Execute the client never described.
        """
        w = self.wire

        # Carried: the row shape is stated, then the empty count.
        messages = w.query(f"SELECT big FROM {DB}.{WIDE_TABLE}")
        self.assert_no_error(messages, "a SELECT whose only column is a hugeint")
        self.assert_equal(kinds(messages), [ROW_DESCRIPTION, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "a one-column hugeint resultset is described, not completed bare")
        carried = row_description_fields(first(messages, ROW_DESCRIPTION))
        self.assert_equal(len(carried), 1, "the resultset describes the one column it selected")
        self.assert_hugeint_field(carried[0], "simple Query of one hugeint column")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "SELECT 0", "the table is empty")

        # Unmapped, simple route: the refusal alone — no row shape, no row,
        # nothing that looks like a completed statement, no stale message after.
        messages = w.query(f"SELECT big FROM {DB}.{UWIDE_TABLE}")
        self.assert_equal(kinds(messages), [ERROR_RESPONSE, READY_FOR_QUERY],
                          "a SELECT whose only column is unencodable")
        self.assert_no_trailing_message(w, "a SELECT whose only column is unencodable")
        message = self.assert_unsupported_column_error(messages, "simple Query of one unencodable column",
                                                       encoding="text")

        # Unmapped, extended protocol without Describe (the client never asked
        # for the row shape): the Execute is refused all the same.
        w.parse("uwide_only", f"SELECT big FROM {DB}.{UWIDE_TABLE}")
        w.bind("", "uwide_only")
        w.execute("", 0)
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(kinds(messages), [PARSE_COMPLETE, BIND_COMPLETE, ERROR_RESPONSE, READY_FOR_QUERY],
                          "a refused Execute of a SELECT whose only column is unencodable")
        assert COMMAND_COMPLETE not in kinds(messages), "the statement must not complete as a DML"
        assert DATA_ROW not in kinds(messages), "no row may be sent for an unencodable column"
        executed = self.assert_unsupported_column_error(messages, "Execute of one unencodable column",
                                                        encoding="text")
        # The Bind requested no result format, so both routes encode in text:
        # the refusal does not depend on which one the client took.
        self.assert_equal(executed, message, "the same refusal on the simple and the extended route")

        # The connection keeps serving.
        messages = w.query("SELECT 1")
        self.assert_no_error(messages, "SELECT 1 after the refusal")
        self.assert_equal(kinds(messages), [ROW_DESCRIPTION, DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "SELECT 1 on the same connection")
        print(f"  SELECT of one column -> described (hugeint) / 0A000 '{message}' (uhugeint) ✓")

    def test_terminate_with_an_unexecuted_named_statement_keeps_the_server_serving(self):
        """Parse a named statement, never execute it, Terminate or drop the socket: the server keeps serving."""
        for how in ("Terminate", "socket drop"):
            side = self.connect()
            side.parse("orphan", f"SELECT id, name FROM {DB}.{TABLE} WHERE id = $1", [OID_INT8])
            side.sync()
            self.assert_no_error(side.read_until_ready(), f"Parse on the side connection ({how})")
            if how == "Terminate":
                side.terminate()
            side.close()

        # The shared connection is still served after both teardowns.
        messages = self.wire.query(f"SELECT id FROM {DB}.{TABLE}")
        self.assert_no_error(messages, "the shared connection after two teardowns")
        self.assert_equal(kinds(messages).count(DATA_ROW), ROWS, "rows on the shared connection")

        # A fresh connection is accepted and runs the same statement name end to end.
        fresh = self.connect()
        fresh.parse("orphan", f"SELECT id, name FROM {DB}.{TABLE} WHERE id = $1", [OID_INT8])
        fresh.bind("", "orphan", ["2"])
        fresh.execute("", 0)
        fresh.sync()
        messages = fresh.read_until_ready()
        self.assert_no_error(messages, "a fresh connection after the teardowns")
        self.assert_equal([data_row_texts(p)[1] for k, p in messages if k == DATA_ROW], ["row_2"],
                          "the fresh connection's statement runs")
        fresh.terminate()
        fresh.close()
        print("  named Parse then Terminate / socket drop: server keeps serving ✓")

    def test_malformed_message_is_a_protocol_error_not_a_disconnect(self):
        """A truncated Execute / an unterminated Query: ErrorResponse ERROR 08P01, the connection lives on."""
        w = self.wire

        # Inside a pipeline: Parse and Bind complete, the truncated Execute is
        # the error, the following Execute is discarded, the Sync answers the
        # one ReadyForQuery.
        w.parse("", f"SELECT id FROM {DB}.{TABLE} ORDER BY id")
        w.bind("", "")
        w.send_raw(b"E", _cstr("") + struct.pack("!h", 0))   # 2 of the 4 row-limit bytes
        w.execute("", 0)
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(kinds(messages), [PARSE_COMPLETE, BIND_COMPLETE, ERROR_RESPONSE, READY_FOR_QUERY],
                          "a truncated Execute inside a pipeline")
        fields = error_fields(first(messages, ERROR_RESPONSE))
        self.assert_equal(fields.get("S"), "ERROR", "a malformed message after startup is not FATAL")
        self.assert_equal(fields.get("C"), SQLSTATE_PROTOCOL_VIOLATION, "sqlstate of a malformed message")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"I", "the Sync reports the failed pipeline")

        # The simple protocol: ErrorResponse and ReadyForQuery at once.
        w.send_raw(b"Q", b"SELECT 1")                          # no terminator
        messages = w.read_until_ready()
        self.assert_equal(kinds(messages), [ERROR_RESPONSE, READY_FOR_QUERY], "an unterminated Query")
        self.assert_no_trailing_message(w, "an unterminated Query")
        fields = error_fields(first(messages, ERROR_RESPONSE))
        self.assert_equal(fields.get("S"), "ERROR", "an unterminated Query is not FATAL")
        self.assert_equal(fields.get("C"), SQLSTATE_PROTOCOL_VIOLATION, "sqlstate of an unterminated Query")

        # The same connection keeps serving, simple and extended alike.
        messages = w.query("SELECT 1")
        self.assert_no_error(messages, "SELECT 1 after the malformed messages")
        self.assert_equal(kinds(messages), [ROW_DESCRIPTION, DATA_ROW, COMMAND_COMPLETE, READY_FOR_QUERY],
                          "SELECT 1 on the same connection")
        w.parse("", f"SELECT id FROM {DB}.{TABLE} ORDER BY id")
        w.bind("", "")
        w.execute("", 0)
        w.sync()
        messages = w.read_until_ready()
        self.assert_no_error(messages, "extended protocol after the malformed messages")
        self.assert_equal(kinds(messages).count(DATA_ROW), ROWS, "the table still streams")
        print("  truncated Execute / unterminated Query -> ERROR 08P01, connection alive ✓")

    def test_ready_for_query_reports_the_transaction_status(self):
        """ReadyForQuery: I outside a block, T after BEGIN, E once an error failed the block, I after it ends."""
        w = self.wire

        # Outside a transaction block an error fails nothing: the Sync says idle.
        w.send_raw(b"E", _cstr("") + struct.pack("!h", 0))   # 2 of the 4 row-limit bytes
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(kinds(messages), [ERROR_RESPONSE, READY_FOR_QUERY], "a failed pipeline outside a block")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"I", "no block is open")

        # Inside the block: T on the simple Query and on the Sync.
        messages = w.query("BEGIN")
        self.assert_no_error(messages, "BEGIN")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"T", "BEGIN opens a block")
        messages = w.query("SELECT 1")
        self.assert_no_error(messages, "SELECT 1 inside the block")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"T", "a served query keeps the block open")
        w.sync()
        messages = w.read_until_ready()
        self.assert_equal(kinds(messages), [READY_FOR_QUERY], "a Sync alone answers ReadyForQuery")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"T", "the Sync reports the open block")

        # An error inside the pipeline fails the block: the Sync says E, ROLLBACK ends it.
        w.send_raw(b"E", _cstr("") + struct.pack("!h", 0))
        w.sync()
        messages = self.read_failed_pipeline(w)
        self.assert_equal(first(messages, READY_FOR_QUERY), b"E", "an error inside the block fails it")
        messages = w.query("ROLLBACK")
        self.assert_no_error(messages, "ROLLBACK of the failed block")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"I", "ROLLBACK ends the failed block")

        # An error of a simple Query inside the block: E at once; COMMIT rolls the failed block back.
        self.assert_no_error(w.query("BEGIN"), "BEGIN")
        w.send_raw(b"Q", b"SELECT 1")                          # no terminator
        messages = w.read_until_ready()
        self.assert_equal(kinds(messages), [ERROR_RESPONSE, READY_FOR_QUERY], "an unterminated Query inside the block")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"E", "the simple Query reports the failed block")
        messages = w.query("COMMIT")
        self.assert_no_error(messages, "COMMIT of the failed block")
        self.assert_equal(command_tag(first(messages, COMMAND_COMPLETE)), "ROLLBACK",
                          "COMMIT of a failed block rolls it back")
        self.assert_equal(first(messages, READY_FOR_QUERY), b"I", "the block is over")
        print("  ReadyForQuery I / T / E follows the transaction block ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_execute_row_limit_suspends_the_portal()
            self.test_describe_statement_reports_parameter_oids()
            self.test_describe_statement_expands_a_star_before_bind()
            self.test_describe_portal_describes_the_executed_result()
            self.test_describe_portal_of_a_dml_does_not_execute_it()
            self.test_describe_statement_of_a_returning_dml_names_its_columns()
            self.test_execute_refuses_a_result_shape_the_describe_did_not_promise()
            self.test_named_statement_executes_twice_and_after_a_failure()
            self.test_bind_rejects_a_literal_the_type_cannot_hold()
            self.test_binary_result_format_is_reported_in_row_description()
            self.test_hugeint_column_is_carried_as_numeric_in_text_and_refused_in_binary()
            self.test_unsupported_column_type_is_an_error_not_a_disconnect()
            self.test_select_of_a_single_column_is_never_a_bare_command_complete()
            self.test_terminate_with_an_unexecuted_named_statement_keeps_the_server_serving()
            self.test_malformed_message_is_a_protocol_error_not_a_disconnect()
            self.test_ready_for_query_reports_the_transaction_status()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="PG extended-protocol wire contract")
    parser.add_argument("--local", action="store_true", help="Use local host instead of test-otterstax")
    args = parser.parse_args()

    tests = PgExtendedProtocolTest(local=args.local)
    try:
        tests.run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - PG extended protocol (raw wire)\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - PG extended protocol (raw wire)\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
