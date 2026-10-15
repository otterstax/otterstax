"""Raw PostgreSQL batch transaction checks for the owned remote fixture."""
from test_pg_extended_protocol import PgWire, data_row_texts, error_fields


class PgTransactionBatchTest:
    def __init__(self, ports, table, reset, rows):
        self.ports = ports
        self.table = table
        self.reset = reset
        self.rows = rows
        self.wire = None

    def setup(self):
        self.reset()
        self.wire = PgWire("127.0.0.1", self.ports["postgres"])
        self.wire.startup("test", "test")

    def cleanup(self):
        if self.wire is not None:
            self.wire.close()

    def batch(self, sql, statement="", portal=""):
        self.wire.parse(statement, sql)
        self.wire.bind(portal, statement)
        self.wire.execute(portal, 0)

    def read_command(self):
        self.wire.send_raw(b"H", b"")
        messages = []
        while True:
            kind, body = self.wire.read_message()
            messages.append((kind, body))
            assert kind != b"E", error_fields(body) if kind == b"E" else messages
            if kind == b"C":
                return messages

    def sync(self, status=b"I", error=None):
        self.wire.sync()
        messages = self.wire.read_until_ready()
        assert messages[-1] == (b"Z", status), messages
        failures = [error_fields(body) for kind, body in messages if kind == b"E"]
        if error is None:
            assert failures == [], failures
        else:
            assert len(failures) == 1, failures
            if error != "any":
                assert failures[0]["C"] == error, failures
        return messages

    def query(self, sql, status=b"I"):
        messages = self.wire.query(sql)
        assert not any(kind == b"E" for kind, _ in messages), messages
        assert messages[-1] == (b"Z", status), messages
        return messages

    def test_flush_retains_writes_until_sync(self):
        """Flush leaves writes invisible; Sync commits the batch."""
        self.wire.parse("insert", f"INSERT INTO {self.table} (id) VALUES ($1)", [20])
        self.wire.bind("first", "insert", ["1"])
        self.wire.execute("first", 0)
        self.read_command()
        assert self.rows() == [], "Flush committed an implicit transaction"
        self.sync()
        assert self.rows() == [(1,)]
        print(f"PASS PG batches {self.table}: Flush retains writes and Sync commits", flush=True)

    def test_failure_rolls_back_and_skips(self):
        """A failed batch undoes earlier writes and skips queued messages."""
        self.batch(f"INSERT INTO {self.table} VALUES (2)")
        self.read_command()
        self.wire.parse("bad", f"SELECT * FROM {self.table}_missing")
        self.batch(f"INSERT INTO {self.table} VALUES (3)", "skipped")
        self.sync(error="any")
        assert self.rows() == [(1,)], "Failed batch retained earlier or skipped writes"
        print(f"PASS PG batches {self.table}: error rolls back earlier writes and skips to Sync", flush=True)

    def test_begin_promotes_implicit_prefix(self):
        """BEGIN retains earlier implicit writes inside the explicit block."""
        self.batch(f"INSERT INTO {self.table} VALUES (4)")
        self.read_command()
        self.batch("BEGIN", "promote")
        self.sync(b"T")
        assert self.rows() == [(1,)]
        self.query("ROLLBACK")
        assert self.rows() == [(1,)], "BEGIN committed the implicit prefix"
        print(f"PASS PG batches {self.table}: BEGIN promotes the uncommitted prefix", flush=True)

    def test_explicit_block_survives_sync(self):
        """Sync does not commit an explicit transaction."""
        self.query("BEGIN", b"T")
        self.batch(f"INSERT INTO {self.table} VALUES (5)")
        self.sync(b"T")
        assert self.rows() == [(1,)], "Sync committed an explicit block"
        self.query("ROLLBACK")
        assert self.rows() == [(1,)]
        print(f"PASS PG batches {self.table}: explicit transaction survives Sync", flush=True)

    def test_statement_and_portal_lifetimes(self):
        """Prepared statements survive Sync; completed transaction portals do not."""
        self.wire.bind("reused", "insert", ["6"])
        self.wire.execute("reused", 0)
        self.sync()
        assert self.rows() == [(1,), (6,)]
        self.wire.execute("reused", 0)
        self.sync(error="any")
        assert self.rows() == [(1,), (6,)]
        print(f"PASS PG batches {self.table}: statements persist and portals expire at transaction end", flush=True)

    def test_rebound_portal_ownership(self):
        """Closing an old statement preserves the portal rebound to another."""
        # Rebinding the unnamed portal removes its relationship to statement A.
        self.wire.parse("owner_a", "SELECT 1")
        self.wire.bind("", "owner_a")
        self.wire.parse("owner_b", "SELECT 2")
        self.wire.bind("", "owner_b")
        self.wire.send_raw(b"C", b"Sowner_a\x00")
        self.wire.execute("", 0)
        messages = self.sync()
        assert [data_row_texts(body) for kind, body in messages if kind == b"D"] == [["2"]]
        print(f"PASS PG batches {self.table}: closing an old statement preserves a rebound portal", flush=True)

    def test_failed_block_refuses_cached_describe(self):
        """An aborted transaction cannot expose cached portal rows."""
        self.query("BEGIN", b"T")
        self.wire.parse("cached", f"SELECT id FROM {self.table}")
        self.wire.bind("cached_portal", "cached")
        self.wire.describe("P", "cached_portal")
        self.sync(b"T")
        self.wire.parse("fail_explicit", f"SELECT * FROM {self.table}_missing")
        self.sync(b"E", "any")
        self.wire.describe("P", "cached_portal")
        self.sync(b"E", "25P02")
        self.query("ROLLBACK")
        print(f"PASS PG batches {self.table}: aborted blocks cannot describe cached rows", flush=True)

    def test_malformed_sync_rolls_back(self):
        """A malformed Sync rolls back the batch and resynchronizes the wire."""
        self.batch(f"INSERT INTO {self.table} VALUES (8)")
        self.read_command()
        self.wire.send_raw(b"S", b"invalid")
        messages = self.wire.read_until_ready()
        assert [kind for kind, _ in messages] == [b"E", b"Z"], messages
        assert error_fields(messages[0][1])["C"] == "08P01"
        assert messages[-1] == (b"Z", b"I")
        assert self.rows() == [(1,), (6,)]
        print(f"PASS PG batches {self.table}: malformed Sync rolls back and resynchronizes", flush=True)

    def test_query_completes_pending_batch(self):
        """A simple Query completes or aborts the pending implicit transaction."""
        self.batch(f"INSERT INTO {self.table} VALUES (9)")
        self.read_command()
        self.query(f"INSERT INTO {self.table} VALUES (10)")
        assert self.rows() == [(1,), (6,), (9,), (10,)]
        self.batch(f"INSERT INTO {self.table} VALUES (11)")
        self.read_command()
        messages = self.wire.query(f"SELECT * FROM {self.table}_missing")
        assert [kind for kind, _ in messages] == [b"E", b"Z"], messages
        assert messages[-1] == (b"Z", b"I")
        assert self.rows() == [(1,), (6,), (9,), (10,)]
        print(f"PASS PG batches {self.table}: simple Query completes the pending implicit transaction", flush=True)

    def test_disconnect_releases_pending_writes(self):
        """Disconnect rolls back the implicit transaction and releases row locks."""
        self.batch(f"INSERT INTO {self.table} VALUES (7)")
        self.read_command()
        self.wire.terminate()
        self.wire.close()
        self.wire = PgWire("127.0.0.1", self.ports["postgres"])
        self.wire.startup("test", "test")
        self.query(f"INSERT INTO {self.table} VALUES (7)")
        assert self.rows() == [(1,), (6,), (7,), (9,), (10,)]
        print(f"PASS PG batches {self.table}: disconnect rolls back pending writes and releases locks", flush=True)

    def run_all_tests(self):
        try:
            self.setup()
            self.test_flush_retains_writes_until_sync()
            self.test_failure_rolls_back_and_skips()
            self.test_begin_promotes_implicit_prefix()
            self.test_explicit_block_survives_sync()
            self.test_statement_and_portal_lifetimes()
            self.test_rebound_portal_ownership()
            self.test_failed_block_refuses_cached_describe()
            self.test_malformed_sync_rolls_back()
            self.test_query_completes_pending_batch()
            self.test_disconnect_releases_pending_writes()
        finally:
            self.cleanup()
