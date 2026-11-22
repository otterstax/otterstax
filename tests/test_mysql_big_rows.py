# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026  OtterStax

"""Big rows and packet bounds on the MySQL wire, against the real stack.

The unit tests pin the packet bytes; this file pins what a real driver does
with them, over a real backend:

  * A client advertising max_packet_size = 0 in HandshakeResponse41 — what
    go-sql-driver/mysql and mysql2 send — is served, engine and backend alike
    (the board's P1: the handshake value bounds nothing the server reads).
  * A row whose payload exceeds one wire packet leaves in pieces of
    MAX_PACKET_SIZE (0xFFFFFF). PyMySQL reassembles a payload from the piece
    length alone (a piece of exactly 0xFFFFFF means "more follows"), so it
    must read the split row back whole — and the row behind a split one must
    stay its own row, not a continuation of it.
  * A command of one full wire packet (a payload of MAX_PACKET_LEN = 16777215
    bytes plus SQL overhead) is taken, exactly as MariaDB takes it:
    @@max_allowed_packet announces 16MB, like MariaDB's default.
  * A command PyMySQL splits over several wire packets is reassembled: every
    packet but the last carries MAX_PACKET_LEN, the last is shorter (or empty,
    when the payload fills MAX_PACKET_LEN exactly — a compliant client ends
    such a payload with an empty terminating packet), and the pieces execute
    as one statement. A command larger than @@max_allowed_packet tears the
    connection with ER 1153, as MariaDB tears a client that sends a packet
    bigger than max_allowed_packet — and the next connection is served.

The big cell is built on MariaDB itself (REPEAT), so the megabytes cross only
the OtterStax → client direction, once per read. The reassembly cases are
constant SELECTs: a two-packet command's payload is already 16MB of statement
text, and nothing of it can be stored — the engine refuses a string bigger
than 262132 bytes — so the proof that the pieces became one statement is the
statement's own answer, cut inside its number.

Runs in the compose network (server container test-otterstax) and on the host
against a locally run server with the docker backends (`--local`).
"""

import argparse
import socket
import struct
import sys

import pymysql

import config

OK_HEADER = 0x00
ERR_HEADER = 0xFF
CLIENT_PROTOCOL_41 = 1 << 9
CLIENT_CONNECT_WITH_DB = 1 << 3
CLIENT_SECURE_CONNECTION = 1 << 15
CLIENT_PLUGIN_AUTH = 1 << 19
MAX_PACKET_LEN = 0xFFFFFF          # the protocol's per-packet payload cap
MAX_ALLOWED_PACKET = 16 * 1024 * 1024   # @@max_allowed_packet: the reassembly bound
# A cell whose row payload is 16777219 bytes (9-byte length prefix + the cell):
# one full 0xFFFFFF piece, then a 4-byte remainder — just over the boundary.
BIG_LEN = MAX_PACKET_LEN - 9 + 4

MARIA_HOST_DOCKER = "mariadb1"
MARIA_HOST_LOCAL = "127.0.0.2"
MARIA_PORT = 3306

PLACE = "campaigns.db1"
TABLE = "big_rows"


class RawClient:
    """A hand-framed MySQL wire client, for what no driver can send."""

    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=10)

    def read_exact(self, n):
        out = bytearray()
        while len(out) < n:
            chunk = self.sock.recv(n - len(out))
            if not chunk:
                raise ConnectionError("server closed the connection")
            out += chunk
        return bytes(out)

    def read_packet(self):
        header = self.read_exact(4)
        length = header[0] | (header[1] << 8) | (header[2] << 16)
        payload = self.read_exact(length)
        return payload

    def send_packet(self, payload, seq):
        frame = struct.pack("<I", len(payload))[:3] + bytes([seq]) + payload
        self.sock.sendall(frame)

    def handshake(self, max_packet_size):
        greeting = self.read_packet()
        assert greeting[0] == 10, f"expected HandshakeV10, got protocol {greeting[0]}"
        flags = (CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
                 | CLIENT_CONNECT_WITH_DB)
        response = struct.pack("<IIB", flags, max_packet_size, 33) + b"\x00" * 23
        response += b"test\x00"
        response += b"\x00"                 # empty auth response (1-byte length)
        response += PLACE.encode() + b"\x00"
        response += b"mysql_native_password\x00"
        self.send_packet(response, 1)
        ok = self.read_packet()
        assert ok[0] == OK_HEADER, f"handshake refused: {ok[0]} {ok[:32]!r}"

    def query(self, sql):
        self.send_packet(b"\x03" + sql.encode(), 0)
        first = self.read_packet()
        assert first[0] != ERR_HEADER, f"SELECT {sql!r} refused: {first[:64]!r}"
        column_count = first[0]
        assert column_count < 251
        for _ in range(column_count):
            self.read_packet()              # column definitions
        eof = self.read_packet()
        assert eof[0] == 0xFE, f"expected EOF after columns, got {eof[0]}"
        rows = []
        while True:
            packet = self.read_packet()
            if packet[0] == 0xFE and len(packet) < 9:
                return rows                 # EOF: the end of the resultset
            row = []
            pos = 0
            while pos < len(packet):
                mark = packet[pos]
                pos += 1
                if mark == 0xFB:
                    row.append(None)
                elif mark == 0xFC:
                    length = packet[pos] | (packet[pos + 1] << 8)
                    pos += 2
                    row.append(packet[pos:pos + length])
                    pos += length
                elif mark == 0xFD:
                    length = packet[pos] | (packet[pos + 1] << 8) | (packet[pos + 2] << 16)
                    pos += 3
                    row.append(packet[pos:pos + length])
                    pos += length
                elif mark == 0xFE:
                    length = int.from_bytes(packet[pos:pos + 8], "little")
                    pos += 8
                    row.append(packet[pos:pos + length])
                    pos += length
                else:
                    row.append(packet[pos:pos + mark])
                    pos += mark
            rows.append(row)

    def close(self):
        self.sock.close()


class MySqlBigRowsTest:
    def __init__(self, local):
        self.local = local
        self.host = config.get_host(local)
        self.maria_host = MARIA_HOST_LOCAL if local else MARIA_HOST_DOCKER
        self.conn = None

    def otterstax(self, **kwargs):
        return pymysql.connect(host=self.host, port=config.MYSQL_PORT, user="test",
                               database=PLACE, autocommit=True, **kwargs)

    def maria(self):
        return pymysql.connect(host=self.maria_host, port=MARIA_PORT, user="user1",
                               password="password1", database="db1", autocommit=True)

    def setup(self):
        # The table is created on MariaDB directly: a LONGTEXT column is what
        # holds a 16MB cell, and the DDL through OtterStax would make it a
        # 64KB TEXT. OtterStax discovers the table by the first statement
        # naming it (the P1 case's count).
        direct = self.maria()
        try:
            with direct.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
                cur.execute(f"CREATE TABLE {TABLE} (id int, payload longtext)")
        finally:
            direct.close()
        self.conn = self.otterstax()

    def cleanup(self):
        # A case may have torn its connection (the over-limit one does): the
        # close tolerates whatever the server did to it.
        conn, self.conn = self.conn, None
        if conn is not None:
            try:
                conn.close()
            except pymysql.err.MySQLError:
                pass
        direct = self.maria()
        try:
            with direct.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        finally:
            direct.close()

    def test_p1_zero_max_packet_client_is_served(self):
        """A client advertising no packet size reaches the engine and the backend."""
        client = RawClient(self.host, config.MYSQL_PORT)
        try:
            client.handshake(max_packet_size=0)
            rows = client.query("SELECT 1")
            assert rows == [[b"1"]], f"SELECT 1 answered {rows!r}"
            rows = client.query(f"SELECT count(*) FROM {TABLE}")
            assert rows == [[b"0"]], f"backend count answered {rows!r}"
        finally:
            client.close()

    def test_big_row_splits_at_max_packet_size_and_pymysql_reassembles(self):
        """A >0xFFFFFF row goes out in pieces; PyMySQL reads it whole, the next row apart."""
        big = "x" * BIG_LEN
        with self.conn.cursor() as cur:
            cur.execute(f"INSERT INTO {TABLE} (id, payload) VALUES (2, 'small')")
        # The cell is built on MariaDB directly (the engine has no `repeat` to
        # push and hauling 16MB in would be the very bulk this test avoids) —
        # in two steps, because cell + SQL overhead must stay inside MariaDB's
        # own 16MB packet: INSERT the first half, CONCAT the second onto it.
        # The megabytes never cross OtterStax on the way in.
        direct = self.maria()
        try:
            with direct.cursor() as cur:
                cur.execute(f"INSERT INTO {TABLE} (id, payload) VALUES (1, %s)",
                            ("x" * (BIG_LEN // 2),))
                cur.execute(f"UPDATE {TABLE} SET payload = CONCAT(payload, %s) WHERE id = 1",
                            ("x" * (BIG_LEN - BIG_LEN // 2),))
        finally:
            direct.close()
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT id, payload FROM {TABLE} ORDER BY id")
            rows = cur.fetchall()
        assert len(rows) == 2, f"expected 2 rows, got {len(rows)}"
        assert rows[0][0] == 1 and rows[0][1] == big, \
            f"the split row came back wrong: id={rows[0][0]}, len={len(rows[0][1] or '')}"
        assert rows[1] == (2, "small"), \
            f"the row behind the split one is not its own row: {rows[1]!r}"

    def test_command_of_one_wire_packet_is_taken_as_mariadb_takes_it(self):
        """A command that fills exactly one wire packet goes through, as MariaDB takes the same.

        A command whose whole payload is one packet runs as it did before
        reassembly: @@max_allowed_packet announces 16MB like MariaDB's
        default, and a packet can never carry more than MAX_PACKET_LEN
        (0xFFFFFF) — the sizes PyMySQL and mysql2 bound their own writes by
        are all inside. The parameter is padded so the whole command stays
        MAX_PACKET_LEN minus 132 bytes: four of command overhead (the
        COM_QUERY byte, the closing quote, the closing paren) and 128 of
        headroom for the pushed statement's own spelling, which must stay
        inside MariaDB's 16MB packet too. Exactly one packet, no continuation.

        A payload of exactly MAX_PACKET_LEN (its empty-terminator shape) is a
        case of its own:
        test_command_of_exactly_max_packet_len_ends_at_the_empty_terminator
        pins that the server takes the empty packet as the command's end and
        answers with the sequence after it.
        """
        prefix = f"INSERT INTO {TABLE} (id, payload) VALUES (3, '"
        # ...minus another 128 for the pushed statement's own spelling (the
        # backquoted names make it slightly longer than the client's), which
        # must stay inside MariaDB's 16MB packet too.
        param = "x" * (MAX_PACKET_LEN - 132 - len(prefix) - len("')"))
        with self.conn.cursor() as cur:
            cur.execute(prefix + param + "')")
            cur.execute(f"SELECT LENGTH(payload) FROM {TABLE} WHERE id = 3")
            assert cur.fetchall() == ((len(param),),), "the full one-packet command was not taken"
        # And the same command to MariaDB directly: both accept it.
        direct = self.maria()
        try:
            with direct.cursor() as cur:
                cur.execute(f"INSERT INTO {TABLE} (id, payload) VALUES (4, %s)", (param,))
                cur.execute(f"SELECT LENGTH(payload) FROM {TABLE} WHERE id = 4")
                assert cur.fetchall() == ((len(param),),), "MariaDB took the same command"
        finally:
            with direct.cursor() as cur:
                cur.execute(f"DELETE FROM {TABLE} WHERE id IN (3, 4)")
            direct.close()

    def test_command_over_one_wire_packet_is_reassembled_and_executed(self):
        """A command PyMySQL splits over two wire packets executes whole.

        PyMySQL's split: the first packet carries MAX_PACKET_LEN (0xFFFFFF)
        bytes, the rest rides in a shorter terminator — a command whose
        payload is MAX_PACKET_LEN + 1, the most a two-packet command can hold
        inside @@max_allowed_packet. The server reassembles the pieces into
        one statement and answers with the sequence after the terminator's
        own. The statement is a constant SELECT whose cut falls inside its
        number — `1e` alone is a syntax error, `1e3` is 1000, so the answer
        proves the pieces joined byte-exact; nothing of the payload is
        stored, which matters because the engine refuses to store a string
        bigger than 262132 bytes.
        """
        sql = "SELECT " + " " * (MAX_PACKET_LEN - 10) + "1e3"
        assert 1 + len(sql) == MAX_PACKET_LEN + 1, "the command must be exactly one packet plus one byte"
        assert sql[-2:] == "e3", "the cut must fall inside the number"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        # `1e3` is typed DOUBLE: the driver reads the text cell as a float.
        assert rows == ((1000.0,),), f"the reassembled statement answered {rows!r}"
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchall() == ((1,),), "the connection did not survive the reassembled command"

    def test_command_of_exactly_max_packet_len_ends_at_the_empty_terminator(self):
        """A command whose payload fills MAX_PACKET_LEN exactly ends at an empty packet.

        A compliant client (PyMySQL, observed live) ends such a payload with
        an empty terminating packet: the length rule reads 0xFFFFFF as "more
        follows". The server takes the empty packet as the command's end,
        never as a command of its own, and answers with the sequence after
        the terminator's own — the client sees the answer, not a sequence
        error. The statement is a constant SELECT (nothing of the payload can
        be stored: the engine refuses a string bigger than 262132 bytes).
        """
        sql = "SELECT 1000 AS one " + " " * (MAX_PACKET_LEN - 20)
        assert 1 + len(sql) == MAX_PACKET_LEN, "the command must fill exactly one packet"
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        assert rows == ((1000,),), f"the reassembled statement answered {rows!r}"

    def test_command_over_max_allowed_packet_tears_the_connection(self):
        """A command larger than @@max_allowed_packet tears the connection (ER 1153).

        MariaDB answers a client that sends a packet bigger than
        max_allowed_packet by refusing and closing; the reassembly bound is
        @@max_allowed_packet, and the same answer keeps the server from
        reading an unbounded stream: nothing past the packet that crosses the
        bound is ever read. The size here makes the crossing packet the last
        one the client sends, so the server's ER 1153 arrives with exactly
        the sequence the driver waits for — the client sees the refusal, not
        a sequence error — the connection is gone, and a fresh one is served
        as usual.
        """
        big = "x" * 20000000
        # The shape this pins: one full packet, then the packet whose arrival
        # crosses the bound — the last one the client sends, so the refusal's
        # sequence is the one the driver waits for.
        prefix = f"INSERT INTO {TABLE} (id, payload) VALUES (5, '"
        total = 1 + len(prefix) + len(big) + len("')")
        assert total > MAX_ALLOWED_PACKET, "the command must cross @@max_allowed_packet"
        assert total - MAX_PACKET_LEN <= MAX_PACKET_LEN, "the crossing packet must be the last"
        conn = self.otterstax()
        try:
            with conn.cursor() as cur:
                raised = None
                try:
                    cur.execute(f"INSERT INTO {TABLE} (id, payload) VALUES (5, %s)", (big,))
                except pymysql.err.OperationalError as err:
                    raised = err.args[0]
                assert raised is not None, f"a {len(big)}-byte command was accepted"
                # The connection did not survive the tear.
                reuse_failed = None
                try:
                    cur.execute("SELECT 1")
                except (pymysql.err.InterfaceError, pymysql.err.OperationalError) as err:
                    reuse_failed = err
                assert reuse_failed is not None, "the connection survived an over-limit command"
        finally:
            try:
                conn.close()
            except pymysql.err.MySQLError:
                pass
        probe = self.otterstax()
        try:
            with probe.cursor() as cur:
                cur.execute("SELECT 1")
                assert cur.fetchall() == ((1,),), "the server did not recover from the over-limit command"
        finally:
            probe.close()

    def run_all_tests(self):
        try:
            self.setup()
            self.test_p1_zero_max_packet_client_is_served()
            self.test_big_row_splits_at_max_packet_size_and_pymysql_reassembles()
            self.test_command_of_one_wire_packet_is_taken_as_mariadb_takes_it()
            self.test_command_over_one_wire_packet_is_reassembled_and_executed()
            self.test_command_of_exactly_max_packet_len_ends_at_the_empty_terminator()
            self.test_command_over_max_allowed_packet_tears_the_connection()
        finally:
            self.cleanup()


def main_test():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true",
                        help="server on 127.0.0.1 (locally run), backends in docker")
    args = parser.parse_args()

    test = MySqlBigRowsTest(args.local)
    try:
        test.run_all_tests()
    except Exception as err:
        print(f"FAIL: {type(err).__name__}: {err}")
        return 1
    print("ALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main_test())
