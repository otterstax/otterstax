"""Exercise remote transactions through both native frontends.

Uses the backend settings from a supplied test fixture, creates uniquely named
tables, and runs an owned OtterStax server with temporary data and ports.
"""
import argparse
import json
import subprocess
import tempfile
import threading
import time
import uuid
from pathlib import Path

import psycopg
import pymysql
import yaml

from test_native_transactions import Client, free_port, mysql_execute, mysql_prepare
from test_pg_transaction_batches import PgTransactionBatchTest
from test_pg_extended_protocol import PgExtendedProtocolTest


class Backend:
    def __init__(self, kind, settings, prefix):
        self.kind = kind
        self.settings = settings
        self.table = prefix
        self.unsafe = prefix + "_unsafe"
        self.triggered = prefix + "_triggered"
        self.trigger = prefix + "_trigger"
        if kind == "postgresql":
            self.connection = psycopg.connect(host=settings["host"], port=int(settings["port"]),
                user=settings["username"], password=settings["password"], dbname=settings["database"],
                autocommit=True, connect_timeout=5)
            self.qualified = f'{settings["alias"]}.{settings["database"]}.{settings.get("schema", "public")}'
        else:
            self.connection = pymysql.connect(host=settings["host"], port=int(settings["port"]),
                user=settings["username"], password=settings["password"], database=settings["database"],
                autocommit=True, connect_timeout=5, read_timeout=15, write_timeout=15)
            self.qualified = f'{settings["alias"]}.{settings["database"]}'

    def sql(self, sql, parameters=None):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall()) if cursor.description else []

    def create(self):
        engine = " ENGINE=InnoDB" if self.kind == "mysql" else ""
        self.sql(f"CREATE TABLE {self.table} (id BIGINT PRIMARY KEY){engine}")
        if self.kind == "mysql":
            self.sql(f"CREATE TABLE {self.unsafe} (id BIGINT) ENGINE=MyISAM")
            self.sql(f"CREATE TABLE {self.triggered} (id BIGINT) ENGINE=InnoDB")
            self.sql(f"CREATE TRIGGER {self.trigger} AFTER INSERT ON {self.triggered} FOR EACH ROW "
                     f"INSERT INTO {self.unsafe} VALUES (NEW.id)")

    def rows(self):
        return self.sql(f"SELECT id FROM {self.table} ORDER BY id")

    def cleanup(self):
        if self.kind == "mysql":
            self.sql(f"DROP TRIGGER IF EXISTS {self.trigger}")
            self.sql(f"DROP TABLE IF EXISTS {self.triggered}")
            self.sql(f"DROP TABLE IF EXISTS {self.unsafe}")
        self.sql(f"DROP TABLE IF EXISTS {self.table}")
        self.connection.close()


def check_connection_loss(protocol, ports, backend, label):
    client = Client(protocol, ports)
    table = backend.qualified + "." + backend.table
    expected = backend.rows()
    errors = []
    worker = None
    try:
        client.sql("BEGIN")
        client.sql(f"INSERT INTO {table} VALUES (11)")
        backend.sql("BEGIN")
        backend.sql(f"SELECT id FROM {backend.table} WHERE id=2 FOR UPDATE")

        def blocked_update():
            try:
                client.sql(f"UPDATE {table} SET id=12 WHERE id=2")
            except (psycopg.Error, pymysql.MySQLError) as error:
                errors.append(error)

        worker = threading.Thread(target=blocked_update, daemon=True)
        worker.start()
        deadline = time.monotonic() + 10
        while True:
            # Only terminate the active UPDATE of this fixture's unique table.
            if backend.kind == "postgresql":
                # This observer retains a row lock in an open transaction.
                # Refresh activity so an early poll cannot hide the later UPDATE.
                backend.sql("SELECT pg_stat_clear_snapshot()")
                victims = backend.sql("SELECT pid FROM pg_stat_activity WHERE pid <> pg_backend_pid() "
                    "AND state='active' AND query LIKE %s", (f'UPDATE %"{backend.table}"%',))
            else:
                victims = backend.sql("SELECT ID FROM information_schema.PROCESSLIST WHERE ID <> CONNECTION_ID() "
                    "AND INFO LIKE %s", (f'UPDATE %`{backend.table}`%',))
            if victims:
                assert len(victims) == 1, victims
                victim = int(victims[0][0])
                if backend.kind == "postgresql":
                    assert backend.sql("SELECT pg_terminate_backend(%s)", (victim,)) == [(True,)]
                else:
                    backend.sql(f"KILL CONNECTION {victim}")
                break
            if time.monotonic() >= deadline:
                if backend.kind == "mysql":
                    diagnostics = backend.sql("SELECT ID,STATE,INFO FROM information_schema.PROCESSLIST "
                        "WHERE ID <> CONNECTION_ID() AND (INFO LIKE %s OR INFO LIKE %s)",
                        (f"%{backend.table}%", f"%{backend.table.encode().hex()}%"))
                else:
                    diagnostics = []
                raise AssertionError(f"Fixture UPDATE did not reach backend: {diagnostics!r}; {errors!r}")
            time.sleep(0.02)
        worker.join(timeout=10)
        assert not worker.is_alive(), "Backend connection loss did not finish the request"
        assert len(errors) == 1, "Lost transaction query unexpectedly succeeded"
    finally:
        backend.sql("ROLLBACK")
        if worker is not None:
            worker.join(timeout=10)
        if protocol == "postgres" or client.connection.open:
            client.close()
    assert backend.rows() == expected, "Lost backend connection published transaction writes"
    # A fresh frontend must still work after the failed socket has been removed.
    observer = Client(protocol, ports)
    try:
        assert observer.sql(f"SELECT id FROM {table} ORDER BY id") == expected
    finally:
        observer.close()
    print(f"PASS {label}: backend connection loss rolls back and is never retried", flush=True)


def check(protocol, ports, backend, other):
    table = backend.qualified + "." + backend.table
    client, second = Client(protocol, ports), Client(protocol, ports)
    backend.sql(f"DELETE FROM {backend.table}")
    label = f"{protocol} -> {backend.kind}"
    try:
        client.sql("BEGIN")
        client.sql(f"INSERT INTO {table} VALUES (1)")
        client.check_state(True)
        assert client.sql(f"SELECT id FROM {table}") == [(1,)]
        assert backend.rows() == []
        second.sql("BEGIN")
        second.sql(f"INSERT INTO {table} VALUES (2)")
        assert second.sql(f"SELECT id FROM {table}") == [(2,)]
        second.sql("COMMIT")
        assert backend.rows() == [(2,)]
        client.sql("ROLLBACK")
        client.check_state(False)
        assert backend.rows() == [(2,)]
        print(f"PASS {label}: separate sessions, isolation, commit and rollback", flush=True)

        client.sql("BEGIN")
        client.sql(f"UPDATE {table} SET id=3 WHERE id=2")
        client.sql(f"DELETE FROM {table} WHERE id=3")
        assert client.sql(f"SELECT id FROM {table}") == []
        client.sql("ROLLBACK")
        assert backend.rows() == [(2,)]
        print(f"PASS {label}: UPDATE and DELETE rollback", flush=True)

        client.sql("BEGIN")
        client.sql(f"INSERT INTO {table} VALUES (4)")
        client.expect_error(f"INSERT INTO {table} VALUES (2)")
        client.expect_error(f"INSERT INTO {table} VALUES (5)", "25P02")
        client.sql("COMMIT")
        assert backend.rows() == [(2,)]
        print(f"PASS {label}: backend constraint failure cannot commit earlier writes", flush=True)

        if protocol == "postgres":
            client.sql("BEGIN", prepared=True)
            client.sql(f"INSERT INTO {table} (id) VALUES (%s)", (6,), prepared=True)
            assert client.sql(f"SELECT id FROM {table} ORDER BY id", prepared=True) == [(2,), (6,)]
            client.sql("ROLLBACK", prepared=True)
        else:
            statement = mysql_prepare(client.connection, f"INSERT INTO {table} (id) VALUES (?)")
            client.sql("BEGIN")
            mysql_execute(client.connection, statement, 6)
            client.sql("ROLLBACK")
        assert backend.rows() == [(2,)]
        print(f"PASS {label}: prepared statements retain remote transaction", flush=True)

        client.sql("BEGIN")
        client.sql(f"INSERT INTO {table} VALUES (7)")
        client.expect_error(f"SELECT id FROM {other.qualified}.{other.table}", "0A000")
        client.sql("ROLLBACK")
        assert backend.rows() == [(2,)]
        client.sql("BEGIN")
        client.sql(f"INSERT INTO {table} VALUES (8)")
        client.expect_error("CREATE TABLE local_tx.cross_storage (id BIGINT)", "0A000")
        client.sql("ROLLBACK")
        assert backend.rows() == [(2,)]
        print(f"PASS {label}: cross-participant operations refuse before effects", flush=True)

        if backend.kind == "mysql":
            for unsafe in (backend.unsafe, backend.triggered):
                client.sql("BEGIN")
                client.sql(f"INSERT INTO {table} VALUES (9)")
                client.expect_error(f"INSERT INTO {backend.qualified}.{unsafe} VALUES (9)", "0A000")
                client.sql("ROLLBACK")
                assert backend.rows() == [(2,)]
                assert backend.sql(f"SELECT id FROM {unsafe}") == []
            print(f"PASS {label}: MyISAM and trigger effects are rejected before writes", flush=True)

        check_connection_loss(protocol, ports, backend, label)

        client.sql("BEGIN")
        client.sql(f"INSERT INTO {table} VALUES (10)")
        client.close()
        client = None
        # Cleanup must have reached the backend before a new connection can
        # insert the same key. A leaked transaction instead blocks or times out.
        second.sql(f"INSERT INTO {table} VALUES (10)")
        assert backend.rows() == [(2,), (10,)]
        print(f"PASS {label}: disconnect releases transaction and row locks", flush=True)
    finally:
        if client is not None:
            client.close()
        second.close()


def run(server, fixture):
    settings = yaml.safe_load(fixture.read_text())
    prefix = "otx_tx_" + uuid.uuid4().hex[:12]
    connections = {kind: [dict(settings["connections"][kind][0])] for kind in ("postgresql", "mysql")}
    for kind, entries in connections.items():
        entries[0]["alias"] = "txn_pg" if kind == "postgresql" else "txn_mysql"
        entries[0]["table"] = ""
    backends = []
    try:
        for kind, entries in connections.items():
            backend = Backend(kind, entries[0], prefix)
            backends.append(backend)
            backend.create()
        with tempfile.TemporaryDirectory(prefix="otterstax-remote-transactions-") as directory:
            root = Path(directory)
            ports = {name: free_port() for name in ("mysql", "postgres", "flight_sql", "spark_connect")}
            assert len(set(ports.values())) == len(ports)
            config = {"connections": connections,
                "service": {name: {"host": "127.0.0.1", "port": port} for name, port in ports.items()}}
            config["service"]["security"] = {"mode": "trusted_private"}
            config_file = root / "config.json"
            config_file.write_text(json.dumps(config))
            process = None
            log_file = root / "server.log"
            try:
                with log_file.open("w") as log:
                    process = subprocess.Popen([str(server), "--config", str(config_file), "--data-dir", str(root / "data")],
                        stdout=log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 60
                while True:
                    if process.poll() is not None:
                        raise AssertionError("Server exited during startup")
                    try:
                        setup = Client("postgres", ports)
                        break
                    except psycopg.OperationalError:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(0.1)
                try:
                    setup.sql("CREATE DATABASE local_tx")
                finally:
                    setup.close()
                for protocol in ("postgres", "mysql"):
                    for index, backend in enumerate(backends):
                        check(protocol, ports, backend, backends[1 - index])
                for backend in backends:
                    PgTransactionBatchTest(ports, backend.qualified + "." + backend.table,
                                  lambda b=backend: b.sql(f"DELETE FROM {b.table}"), backend.rows).run_all_tests()
                observer = Client("postgres", ports)
                try:
                    observer.sql("CREATE TABLE local_tx.batches (id BIGINT PRIMARY KEY)")
                    PgTransactionBatchTest(ports, "local_tx.batches",
                                  lambda: observer.sql("DELETE FROM local_tx.batches"),
                                  lambda: observer.sql("SELECT id FROM local_tx.batches ORDER BY id")).run_all_tests()
                finally:
                    observer.close()
                PgExtendedProtocolTest(local=True, port=ports["postgres"]).run_all_tests()
            except Exception:
                print(log_file.read_text()[-6000:], flush=True)
                raise
            finally:
                if process is not None:
                    process.terminate()
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
    finally:
        for backend in backends:
            backend.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--backend-config", type=Path, required=True)
    args = parser.parse_args()
    run(args.server.resolve(), args.backend_config.resolve())
