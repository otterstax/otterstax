# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""MySQL wire-protocol contract of the MySQL frontend, on the raw socket.

mysql.connector prepares and executes in one call, so a statement that is
prepared and then abandoned — the shape a client leaves behind when it quits
or drops the socket — cannot be produced through it. The test speaks the
protocol itself (HandshakeResponse41, COM_QUERY, COM_STMT_PREPARE, COM_QUIT)
and asserts on the exact packets the server answers with:

  * A `hugeint` result column is carried, in both result formats. It goes out
    as MySQL's own fixed-point type, NEWDECIMAL (246), whose value travels as a
    string in the binary resultset row exactly as in the text one — so COM_QUERY
    answers a resultset and COM_STMT_PREPARE, whose COM_STMT_EXECUTE answers a
    binary resultset, reports the column under that type with nothing after the
    point (decimals 0).
  * A result column the MySQL wire still cannot encode — a `uhugeint` column,
    for which the frontend maps no type — is an ERR packet (1235, SQLSTATE
    42000) naming the column, its type and the result format the refusal
    applies to (text for COM_QUERY, binary for a prepared statement), and the
    same connection keeps serving queries. This is the refusal path every
    unmapped type takes; before it existed the encoder threw and the connection
    was dropped.
  * A SELECT whose only column is one of those: the carried one is a one-column
    resultset and the refused one is an ERR packet — never an OK packet. A
    result without columns is exactly what the frontend answers with an OK
    packet (a DML), so neither shape may degrade into one.
  * A prepared statement left unexecuted at COM_QUIT, or at a socket drop
    without COM_QUIT: the connection's teardown releases it on the Worker
    (Scheduler::close_statement) and the server keeps accepting and serving
    connections. The release itself is not visible on the wire — this case
    proves the teardown path completes (no crash, no wedged pool slot), not
    the Worker-side erasure.

Everything runs against otterbrix-internal tables, so no backend is involved.
"""

import argparse
import socket
import struct
import sys

from config import get_host, MYSQL_PORT

DB = "mysql_wire_proto"
PLAIN = "plain"
WIDE = "wide"
UWIDE = "uwide"
ROWS = 3

CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000

COM_QUIT = 0x01
COM_QUERY = 0x03
COM_STMT_PREPARE = 0x16

OK_HEADER = 0x00
EOF_HEADER = 0xFE
ERR_HEADER = 0xFF
NULL_MARKER = 0xFB

# Wire types the column definitions are checked against.
MYSQL_TYPE_LONG = 3           # a 32-bit int
MYSQL_TYPE_NEWDECIMAL = 246   # the wire's fixed-point type; a hugeint rides it

ER_NOT_SUPPORTED_YET = 1235
SQLSTATE_COMMAND_ERROR = "42000"


class MysqlWire:
    """The handful of client packets the test needs, framed by hand."""

    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=30)
        self.seq = 0

    def close(self):
        self.sock.close()

    # ── framing ────────────────────────────────────────────────────────────
    def _recv_exact(self, size):
        data = b""
        while len(data) < size:
            chunk = self.sock.recv(size - len(data))
            if not chunk:
                raise ConnectionError("server closed the connection")
            data += chunk
        return data

    def read_packet(self):
        header = self._recv_exact(4)
        length = header[0] | (header[1] << 8) | (header[2] << 16)
        self.seq = header[3]
        return self._recv_exact(length)

    def send_packet(self, payload, seq):
        length = len(payload)
        self.sock.sendall(bytes([length & 0xFF, (length >> 8) & 0xFF, (length >> 16) & 0xFF, seq]) + payload)

    def send_command(self, command, body=b""):
        self.send_packet(bytes([command]) + body, 0)

    # ── handshake ──────────────────────────────────────────────────────────
    def handshake(self, user):
        greeting = self.read_packet()
        assert greeting[0] == 10, f"expected HandshakeV10, got protocol {greeting[0]}"
        flags = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        response = struct.pack("<IIB", flags, 16 * 1024 * 1024, 33) + b"\x00" * 23
        response += user.encode() + b"\x00"
        response += b"\x00"                        # empty auth response (1-byte length)
        response += b"mysql_native_password\x00"
        self.send_packet(response, 1)
        ok = self.read_packet()
        assert ok[0] == OK_HEADER, f"handshake refused: {self.decode_err(ok)}"

    # ── decoding ───────────────────────────────────────────────────────────
    @staticmethod
    def lenenc_int(payload, offset):
        first = payload[offset]
        if first < 0xFB:
            return first, offset + 1
        if first == 0xFC:
            return struct.unpack_from("<H", payload, offset + 1)[0], offset + 3
        if first == 0xFD:
            return payload[offset + 1] | (payload[offset + 2] << 8) | (payload[offset + 3] << 16), offset + 4
        return struct.unpack_from("<Q", payload, offset + 1)[0], offset + 9

    @classmethod
    def lenenc_str(cls, payload, offset):
        length, offset = cls.lenenc_int(payload, offset)
        return payload[offset:offset + length].decode(), offset + length

    @staticmethod
    def decode_err(payload):
        """(code, sqlstate, message) of an ERR packet."""
        assert payload[0] == ERR_HEADER, f"not an ERR packet: 0x{payload[0]:02x}"
        (code,) = struct.unpack_from("<H", payload, 1)
        assert payload[3:4] == b"#", "ERR packet without a SQLSTATE marker"
        return code, payload[4:9].decode(), payload[9:].decode()

    @staticmethod
    def is_eof(payload):
        return payload[0] == EOF_HEADER and len(payload) < 9

    def read_column_definitions(self, count):
        """[{name, type, length, decimals}] for `count` definitions, then the EOF that closes them.

        The fixed-length part of a ColumnDefinition41 is what carries the wire
        type: a 0x0C length marker, the charset, the display length, the type
        byte, the flags and the number of decimals.
        """
        columns = []
        for _ in range(count):
            packet = self.read_packet()
            offset = 0
            for _field in range(4):                # catalog, schema, table, org_table
                _, offset = self.lenenc_str(packet, offset)
            name, offset = self.lenenc_str(packet, offset)
            _org_name, offset = self.lenenc_str(packet, offset)
            assert packet[offset] == 0x0C, "a column definition states 0x0C fixed-length bytes"
            offset += 1 + 2                        # marker + charset
            (length,) = struct.unpack_from("<I", packet, offset)
            offset += 4
            type_byte = packet[offset]
            offset += 1 + 2                        # type + column flags
            columns.append({"name": name, "type": type_byte, "length": length, "decimals": packet[offset]})
        assert self.is_eof(self.read_packet()), "EOF must follow the column definitions"
        return columns

    # ── commands ───────────────────────────────────────────────────────────
    def query(self, sql):
        """COM_QUERY. Returns ('ok', affected) | ('err', (code, sqlstate, msg)) | ('rows', columns, rows)."""
        self.send_command(COM_QUERY, sql.encode())
        first = self.read_packet()
        if first[0] == ERR_HEADER:
            return ("err", self.decode_err(first))
        if first[0] == OK_HEADER:
            return ("ok", first[1])
        column_count, _ = self.lenenc_int(first, 0)
        columns = self.read_column_definitions(column_count)
        rows = []
        while True:
            packet = self.read_packet()
            if self.is_eof(packet):
                return ("rows", columns, rows)
            row, offset = [], 0
            for _ in range(column_count):
                if packet[offset] == NULL_MARKER:
                    row.append(None)
                    offset += 1
                else:
                    value, offset = self.lenenc_str(packet, offset)
                    row.append(value)
            rows.append(row)

    def prepare(self, sql):
        """COM_STMT_PREPARE. Returns ('ok', stmt_id, columns, params) | ('err', (code, sqlstate, msg))."""
        self.send_command(COM_STMT_PREPARE, sql.encode())
        first = self.read_packet()
        if first[0] == ERR_HEADER:
            return ("err", self.decode_err(first))
        assert first[0] == OK_HEADER, f"unexpected COM_STMT_PREPARE response 0x{first[0]:02x}"
        stmt_id, column_count, param_count = struct.unpack_from("<IHH", first, 1)
        params = self.read_column_definitions(param_count) if param_count else []
        columns = self.read_column_definitions(column_count) if column_count else []
        return ("ok", stmt_id, columns, params)

    def quit(self):
        self.send_command(COM_QUIT)


class MysqlWireProtocolTest:
    def __init__(self, local=False):
        self.host = get_host(local)
        self.wire = None
        self.database_created = False
        self.created_tables = []

    # ── helpers ────────────────────────────────────────────────────────────
    def assert_equal(self, actual, expected, msg=""):
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}. {msg}")

    def connect(self):
        wire = MysqlWire(self.host, MYSQL_PORT)
        wire.handshake("testuser")
        return wire

    def run_ok(self, wire, sql):
        result = wire.query(sql)
        assert result[0] != "err", f"{sql!r} failed: {result[1]}"
        return result

    def setup(self):
        self.wire = self.connect()
        self.run_ok(self.wire, f"CREATE DATABASE {DB}")
        self.database_created = True
        self.run_ok(self.wire, f"CREATE TABLE {DB}.{PLAIN} (id bigint, name string)")
        self.created_tables.append(PLAIN)
        values = ", ".join(f"({i}, 'row_{i}')" for i in range(1, ROWS + 1))
        self.run_ok(self.wire, f"INSERT INTO {DB}.{PLAIN} (id, name) VALUES {values}")
        # Both tables stay empty: the column type alone is what is tested, and
        # an empty result keeps its columns. `hugeint` is carried as NEWDECIMAL,
        # `uhugeint` has no wire type and is refused.
        self.run_ok(self.wire, f"CREATE TABLE {DB}.{WIDE} (id INT, big hugeint)")
        self.created_tables.append(WIDE)
        self.run_ok(self.wire, f"CREATE TABLE {DB}.{UWIDE} (id INT, big uhugeint)")
        self.created_tables.append(UWIDE)

    def cleanup(self):
        if self.wire is None:
            return
        try:
            for table in self.created_tables:
                self.wire.query(f"DROP TABLE {DB}.{table}")
            if self.database_created:
                self.wire.query(f"DROP DATABASE {DB}")
        finally:
            self.wire.quit()
            self.wire.close()

    def assert_unsupported_column_error(self, result, what, encoding="text", type_name="UHUGEINT"):
        """The ERR packet of a column the wire cannot encode, with everything it must name.

        The frontend, not the engine, is what refuses: the column reaches it
        typed, and the message names the column, its logical type, the wire and
        the result format that has no encoder for it — `encoding` is the format
        the route asked for: text for COM_QUERY, binary for whatever a prepared
        statement answers.
        """
        kind, err = result[0], result[1]
        self.assert_equal(kind, "err", f"{what}: an unsupported column must be an ERR packet")
        code, sqlstate, message = err
        self.assert_equal(code, ER_NOT_SUPPORTED_YET, f"{what}: error code")
        self.assert_equal(sqlstate, SQLSTATE_COMMAND_ERROR, f"{what}: sqlstate")
        assert "column 'big'" in message, f"{what}: the message must name the column: {message}"
        assert type_name in message, f"{what}: the message must name the type: {message}"
        assert "MySQL wire" in message, f"{what}: the message must name the wire: {message}"
        assert f"cannot encode in {encoding} format" in message, \
            f"{what}: the message must name the {encoding} result format: {message}"
        return message

    def assert_hugeint_column(self, column, what):
        """The column definition a hugeint goes out under: NEWDECIMAL, nothing after the point."""
        self.assert_equal(column["name"], "big", f"{what}: the column keeps its name")
        self.assert_equal(column["type"], MYSQL_TYPE_NEWDECIMAL,
                          f"{what}: a 128-bit integer rides the wire's fixed-point type")
        self.assert_equal(column["decimals"], 0, f"{what}: a whole number has no digits after the point")

    # ── cases ──────────────────────────────────────────────────────────────
    def test_hugeint_column_is_carried_as_a_newdecimal_column(self):
        """COM_QUERY over a hugeint column: a resultset whose column is NEWDECIMAL."""
        w = self.wire
        kind, columns, rows = self.run_ok(w, f"SELECT * FROM {DB}.{WIDE}")
        self.assert_equal(kind, "rows", "a hugeint column is carried, not refused")
        self.assert_equal([c["name"] for c in columns], ["id", "big"], "both columns are described")
        self.assert_equal(columns[0]["type"], MYSQL_TYPE_LONG, "the int column keeps its own wire type")
        self.assert_hugeint_column(columns[1], "COM_QUERY")
        self.assert_equal(rows, [], "the table is empty: the resultset carries columns and no rows")

        kind, columns, rows = self.run_ok(w, f"SELECT id, name FROM {DB}.{PLAIN} ORDER BY id")
        self.assert_equal([r[0] for r in rows], [str(i) for i in range(1, ROWS + 1)], "the plain table still reads")
        print("  COM_QUERY hugeint -> NEWDECIMAL column, connection alive ✓")

    def test_prepare_of_a_hugeint_column_reports_a_newdecimal_column(self):
        """COM_STMT_PREPARE over a hugeint column: OK, the column typed NEWDECIMAL.

        COM_STMT_EXECUTE answers a binary resultset, so the prepare checks the
        schema against the binary encoder — the format this column used to be
        refused in. A NEWDECIMAL field of a binary row is the same
        length-encoded string as in a text row, so the binary route carries it.
        """
        w = self.wire
        prepared = w.prepare(f"SELECT * FROM {DB}.{WIDE}")
        self.assert_equal(prepared[0], "ok", f"a prepare over a hugeint column: {prepared[1:]}")
        columns = prepared[2]
        self.assert_equal(len(columns), 2, "the statement reports both of its columns")
        self.assert_equal(columns[0]["type"], MYSQL_TYPE_LONG, "the int column keeps its own wire type")
        self.assert_hugeint_column(columns[1], "COM_STMT_PREPARE")

        prepared = w.prepare(f"SELECT id, name FROM {DB}.{PLAIN}")
        self.assert_equal(prepared[0], "ok", f"a plain prepare afterwards: {prepared[1:]}")
        self.assert_equal(len(prepared[2]), 2, "the plain statement reports its two columns")
        print("  COM_STMT_PREPARE hugeint -> NEWDECIMAL column definition ✓")

    def test_unsupported_column_is_an_error_packet_not_a_disconnect(self):
        """COM_QUERY / COM_STMT_PREPARE over a uhugeint column: ERR 1235 naming it."""
        w = self.wire
        # An unsigned 128-bit integer is mapped to no MySQL type at all, so it
        # takes the refusal path every unmapped type takes.
        message = self.assert_unsupported_column_error(w.query(f"SELECT * FROM {DB}.{UWIDE}"), "COM_QUERY",
                                                       encoding="text")
        # COM_STMT_EXECUTE answers a binary resultset, so the prepare checks the
        # schema against the binary encoder: the refusal names that format.
        self.assert_unsupported_column_error(w.prepare(f"SELECT * FROM {DB}.{UWIDE}"), "COM_STMT_PREPARE",
                                             encoding="binary")

        kind, columns, rows = self.run_ok(w, "SELECT 1")
        self.assert_equal(kind, "rows", "SELECT 1 after the refusal")
        self.assert_equal(len(rows), 1, "SELECT 1 delivers its row on the same connection")

        kind, columns, rows = self.run_ok(w, f"SELECT id, name FROM {DB}.{PLAIN} ORDER BY id")
        self.assert_equal([r[0] for r in rows], [str(i) for i in range(1, ROWS + 1)], "the plain table still reads")
        print(f"  COM_QUERY uhugeint -> ERR 1235 '{message}', connection alive ✓")

    def test_select_of_a_single_column_is_never_an_ok_packet(self):
        """'SELECT big' alone: a one-column resultset for hugeint, an ERR packet for uhugeint.

        The whole result is one column. A result without columns is exactly what
        the frontend reports with an OK packet — a DML — and the hazard of this
        shape is that it degrades into one, which a client reads as a statement
        that ran fine. The carried column must come back as a resultset and the
        unmapped one as the refusal; neither may be an OK packet.
        """
        w = self.wire
        result = w.query(f"SELECT big FROM {DB}.{WIDE}")
        self.assert_equal(result[0], "rows",
                          f"a SELECT of one carried column must be a resultset, not OK: {result[1:]}")
        kind, columns, rows = result
        self.assert_equal(len(columns), 1, "the resultset describes the one column it selected")
        self.assert_hugeint_column(columns[0], "COM_QUERY of one hugeint column")
        self.assert_equal(rows, [], "the table is empty: columns and no rows")

        prepared = w.prepare(f"SELECT big FROM {DB}.{WIDE}")
        self.assert_equal(prepared[0], "ok", f"a prepare of one hugeint column: {prepared[1:]}")
        self.assert_equal(len(prepared[2]), 1, "the prepared schema carries the one column")
        self.assert_hugeint_column(prepared[2][0], "COM_STMT_PREPARE of one hugeint column")

        # The unmapped column, whose whole result is that one column: the ERR
        # packet, never the OK packet a column-less payload would produce.
        result = w.query(f"SELECT big FROM {DB}.{UWIDE}")
        self.assert_equal(result[0], "err",
                          f"a SELECT of one unencodable column must be an ERR packet, not OK/rows: {result[1:]}")
        message = self.assert_unsupported_column_error(result, "COM_QUERY of one unencodable column",
                                                       encoding="text")
        prepared = w.prepare(f"SELECT big FROM {DB}.{UWIDE}")
        self.assert_equal(prepared[0], "err",
                          f"a prepare of one unencodable column must be an ERR packet: {prepared[1:]}")
        self.assert_unsupported_column_error(prepared, "COM_STMT_PREPARE of one unencodable column",
                                             encoding="binary")

        kind, columns, rows = self.run_ok(w, "SELECT 1")
        self.assert_equal(kind, "rows", "SELECT 1 after the refusal")
        self.assert_equal(len(rows), 1, "SELECT 1 delivers its row on the same connection")
        kind, columns, rows = self.run_ok(w, f"SELECT id, name FROM {DB}.{PLAIN} ORDER BY id")
        self.assert_equal(len(rows), ROWS, "the plain table still reads after the refusals")
        print(f"  SELECT of one column -> resultset (hugeint) / ERR {ER_NOT_SUPPORTED_YET} '{message}' (uhugeint) ✓")

    def test_quit_with_an_unexecuted_prepared_statement_keeps_the_server_serving(self):
        """Prepare, never execute, then COM_QUIT or drop the socket: the server keeps serving."""
        for how in ("COM_QUIT", "socket drop"):
            side = self.connect()
            prepared = side.prepare(f"SELECT id, name FROM {DB}.{PLAIN} WHERE id = ?")
            self.assert_equal(prepared[0], "ok", f"prepare on the side connection ({how}): {prepared[1:]}")
            if how == "COM_QUIT":
                side.quit()
            side.close()

        # The shared connection is still served after both teardowns.
        kind, columns, rows = self.run_ok(self.wire, f"SELECT id FROM {DB}.{PLAIN}")
        self.assert_equal(len(rows), ROWS, "the shared connection after two teardowns")

        # A fresh connection is accepted and can prepare the same text again.
        fresh = self.connect()
        prepared = fresh.prepare(f"SELECT id, name FROM {DB}.{PLAIN} WHERE id = ?")
        self.assert_equal(prepared[0], "ok", f"prepare on a fresh connection: {prepared[1:]}")
        kind, columns, rows = self.run_ok(fresh, f"SELECT id FROM {DB}.{PLAIN}")
        self.assert_equal(len(rows), ROWS, "a fresh connection is served")
        fresh.quit()
        fresh.close()
        print("  COM_STMT_PREPARE then COM_QUIT / socket drop: server keeps serving ✓")

    def run_all_tests(self):
        try:
            self.setup()
            self.test_hugeint_column_is_carried_as_a_newdecimal_column()
            self.test_prepare_of_a_hugeint_column_reports_a_newdecimal_column()
            self.test_unsupported_column_is_an_error_packet_not_a_disconnect()
            self.test_select_of_a_single_column_is_never_an_ok_packet()
            self.test_quit_with_an_unexecuted_prepared_statement_keeps_the_server_serving()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser(description="MySQL wire-protocol contract (raw socket)")
    parser.add_argument("--local", action="store_true", help="Use local host instead of test-otterstax")
    args = parser.parse_args()

    tests = MysqlWireProtocolTest(local=args.local)
    try:
        tests.run_all_tests()
        print("\n" + "=" * 70)
        print("\033[92m✅ ALL TESTS PASSED - MySQL wire protocol (raw socket)\033[0m")
        print("=" * 70)
        return 0
    except Exception as e:
        print("\n" + "=" * 70)
        print("\033[91m❌ TEST FAILED - MySQL wire protocol (raw socket)\033[0m")
        print("=" * 70)
        print(f"\033[91mError: {e}\033[0m")
        return 1
    finally:
        print("\nTest completed.")


if __name__ == "__main__":
    sys.exit(main_test())
