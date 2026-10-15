"""Verify native transactions with psycopg, PyMySQL and binary MySQL prepares.

Runs an owned server, data directory and ports. No existing server data is used.
"""
import argparse
import contextlib
import json
import socket
import struct
import subprocess
import tempfile
import time
from pathlib import Path

import psycopg
import pymysql


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Client:
    def __init__(self, protocol, ports):
        self.protocol = protocol
        if protocol == "postgres":
            self.connection = psycopg.connect(host="127.0.0.1", port=ports[protocol], user="test", dbname="test",
                                               autocommit=True, connect_timeout=2, sslmode="disable")
        else:
            self.connection = pymysql.connect(host="127.0.0.1", port=ports[protocol], user="test", autocommit=True,
                                               connect_timeout=2, read_timeout=15, write_timeout=15)

    def sql(self, sql, parameters=None, prepared=False):
        with self.connection.cursor() as cursor:
            if self.protocol == "postgres":
                cursor.execute(sql, parameters, prepare=prepared)
            else:
                cursor.execute(sql, parameters)
            return list(cursor.fetchall()) if cursor.description else []

    def expect_error(self, sql, state=None):
        try:
            self.sql(sql)
        except (psycopg.Error, pymysql.MySQLError) as error:
            if state and self.protocol == "postgres":
                assert error.sqlstate == state, (sql, error.sqlstate, state)
            return
        raise AssertionError("Query unexpectedly succeeded: " + sql)

    def check_state(self, active):
        if self.protocol == "postgres":
            assert self.connection.info.transaction_status == (psycopg.pq.TransactionStatus.INTRANS if active
                                                                else psycopg.pq.TransactionStatus.IDLE)
        else:
            assert bool(self.connection.server_status & 1) == active, self.connection.server_status

    def close(self):
        self.connection.close()


def mysql_prepare(connection, sql):
    connection._execute_command(0x16, sql)
    packet = connection._read_packet()
    packet.check_error()
    _, statement, columns, parameters, _, _ = struct.unpack("<BIHHBH", packet.get_all_data())
    for count in (parameters, columns):
        if count:
            for _ in range(count):
                connection._read_packet().check_error()
            assert connection._read_packet().is_eof_packet()
    return statement


def mysql_execute(connection, statement, value):
    # COM_STMT_EXECUTE: one signed BIGINT parameter, no nulls, fresh type binding.
    payload = struct.pack("<IBIBBHq", statement, 0, 1, 0, 1, 8, value)
    connection._execute_command(0x17, payload)
    packet = connection._read_packet()
    packet.check_error()
    assert packet.is_ok_packet()


def check(protocol, ports):
    client, observer = Client(protocol, ports), Client("postgres", ports)
    table = f"transactions.{protocol}"
    try:
        client.sql(f"CREATE TABLE {table} (id BIGINT);")
        client.sql("BEGIN;")
        client.check_state(True)
        client.sql(f"INSERT INTO {table} VALUES (1);")
        assert client.sql(f"SELECT * FROM {table};") == [(1,)]
        client.check_state(True)  # includes the resultset's EOF/status field
        assert observer.sql(f"SELECT * FROM {table};") == []
        client.sql("ROLLBACK;")
        client.check_state(False)
        assert observer.sql(f"SELECT * FROM {table};") == []
        print(f"PASS {protocol} isolation and rollback across statements", flush=True)

        client.sql("BEGIN;")
        client.sql(f"INSERT INTO {table} VALUES (2);")
        client.sql("COMMIT;")
        client.check_state(False)
        assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
        client.sql("BEGIN;")
        client.sql(f"UPDATE {table} SET id = 3 WHERE id = 2;")
        client.sql(f"DELETE FROM {table} WHERE id = 3;")
        client.sql("ROLLBACK;")
        assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
        print(f"PASS {protocol} commit and rollback of UPDATE/DELETE", flush=True)

        client.sql("BEGIN;")
        client.sql(f"INSERT INTO {table} VALUES (4);")
        client.expect_error(f"INSERT INTO transactions.absent VALUES (4);")
        client.expect_error(f"INSERT INTO {table} VALUES (5);", "25P02")
        client.sql("COMMIT;")
        client.check_state(False)
        assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
        print(f"PASS {protocol} failed block cannot commit or accept another write", flush=True)

        client.sql("BEGIN;")
        client.sql(f"INSERT INTO {table} VALUES (6);")
        client.expect_error("SAVEPOINT point;", "0A000")
        client.sql("ROLLBACK;")
        assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
        print(f"PASS {protocol} unsupported savepoint fails without false acknowledgement", flush=True)

        if protocol == "postgres":
            client.sql("BEGIN;", prepared=True)
            client.sql(f"INSERT INTO {table} (id) VALUES (%s);", (7,), prepared=True)
            assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
            client.sql("ROLLBACK;", prepared=True)
            client.check_state(False)
            client.sql("BEGIN;", prepared=True)
            client.sql(f"INSERT INTO {table} (id) VALUES (%s);", (8,), prepared=True)
            client.sql("ROLLBACK;", prepared=True)
        else:
            statement = mysql_prepare(client.connection, f"INSERT INTO {table} (id) VALUES (?);")
            client.sql("BEGIN;")
            mysql_execute(client.connection, statement, 7)
            assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
            client.sql("ROLLBACK;")
            client.sql("BEGIN;")
            mysql_execute(client.connection, statement, 8)
            client.sql("ROLLBACK;")
            client.connection._execute_command(0x19, struct.pack("<I", statement))
        assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
        print(f"PASS {protocol} prepared execution and re-execution retain the connection transaction", flush=True)

        if protocol == "mysql":
            client.connection.autocommit(False)
            assert not client.connection.get_autocommit()
            client.sql(f"INSERT INTO {table} VALUES (9);")
            client.check_state(True)
            assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
            client.connection.rollback()
            assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
            client.sql(f"INSERT INTO {table} VALUES (10);")
            client.connection.autocommit(True)
            assert client.connection.get_autocommit()
            client.check_state(False)
            assert observer.sql(f"SELECT * FROM {table} ORDER BY id;") == [(2,), (10,)]
            print("PASS mysql autocommit setting commits and rolls back actual engine work", flush=True)
        else:
            client.connection.autocommit = False
            client.sql(f"INSERT INTO {table} VALUES (9);")
            assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
            client.connection.rollback()
            assert observer.sql(f"SELECT * FROM {table};") == [(2,)]
            client.connection.autocommit = True
            print("PASS postgres driver's automatic transaction and rollback", flush=True)

        expected = observer.sql(f"SELECT * FROM {table} ORDER BY id;")
        client.sql("BEGIN;")
        client.sql(f"INSERT INTO {table} VALUES (99);")
        client.close()
        assert observer.sql(f"SELECT * FROM {table} ORDER BY id;") == expected
        print(f"PASS {protocol} connection close rolls back without publishing its row", flush=True)
    finally:
        with contextlib.suppress(Exception):
            client.close()
        observer.close()


def run(server):
    with tempfile.TemporaryDirectory(prefix="otterstax-transactions-") as directory:
        root = Path(directory)
        ports = {name: free_port() for name in ("mysql", "postgres", "flight_sql", "spark_connect")}
        assert len(set(ports.values())) == len(ports)
        config = {"service": {name: {"host": "127.0.0.1", "port": port} for name, port in ports.items()}}
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
                    raise AssertionError("Server exited: " + log_file.read_text()[-3000:])
                try:
                    setup = Client("postgres", ports)
                    break
                except psycopg.OperationalError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.1)
            try:
                setup.sql("CREATE DATABASE transactions;")
            finally:
                setup.close()
            for protocol in ("postgres", "mysql"):
                check(protocol, ports)
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    run(parser.parse_args().server.resolve())
