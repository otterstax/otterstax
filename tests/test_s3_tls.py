"""Exercise S3 TLS through SQL against an owned HTTPS object fixture and server.

Run with --server /path/to/server. Requires psycopg and openssl. Credentials are
generated in memory and supplied through environment references in configuration.
"""

import argparse
import contextlib
import http.server
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import tempfile
import threading
import time

import psycopg


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def certificate(root, name, hostname):
    cert, key = root / f"{name}.crt", root / f"{name}.key"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-keyout", str(key), "-out", str(cert), "-subj", f"/CN={hostname}",
                    "-addext", f"subjectAltName=DNS:{hostname}"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return cert, key


def run(server):
    with tempfile.TemporaryDirectory(prefix="otterstax-s3-tls-") as directory:
        root = Path(directory)
        cert, key = certificate(root, "storage", "localhost")
        wrong_ca, _ = certificate(root, "untrusted", "untrusted.example")
        objects = {"/bucket/people.csv": b"id,name\n1,Alice\n2,Bob\n"}
        requests = []
        forbidden_requests = []

        class Storage(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def reply(self, send_body):
                requests.append((self.command, self.path))
                path = self.path.split("?", 1)[0]
                if path in ("/bucket/redirect_http.csv", "/bucket/redirect_https.csv"):
                    destination = (f"http://127.0.0.1:{forbidden.server_port}" if "_http." in path
                                   else f"https://localhost:{forbidden_tls.server_port}")
                    self.send_response(307)
                    self.send_header("Location", f"{destination}/bucket/people.csv")
                    self.send_header("Content-Length", "0")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.close_connection = True
                    return
                data = objects.get(self.path)
                self.send_response(200 if data is not None else 404)
                self.send_header("Content-Length", str(len(data or b"")))
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("ETag", '"fixture-etag"')
                self.send_header("Last-Modified", "Mon, 12 Oct 2026 00:00:00 GMT")
                self.send_header("x-amz-bucket-region", "us-east-1")
                self.end_headers()
                if send_body and data:
                    self.wfile.write(data)

            def do_HEAD(self):
                self.reply(False)

            def do_GET(self):
                self.reply(True)

            def do_POST(self):
                self.reply(True)

            def do_PUT(self):
                self.reply(True)

        class Forbidden(Storage):
            def reply(self, send_body):
                forbidden_requests.append((self.command, self.path))
                super().reply(send_body)

        forbidden = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Forbidden)
        forbidden.daemon_threads = True
        forbidden_thread = threading.Thread(target=forbidden.serve_forever, daemon=True)
        forbidden_thread.start()

        storage = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Storage)
        storage.daemon_threads = True
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.minimum_version = ssl.TLSVersion.TLSv1_2
        tls.load_cert_chain(cert, key)
        storage.socket = tls.wrap_socket(storage.socket, server_side=True)
        thread = threading.Thread(target=storage.serve_forever, daemon=True)
        thread.start()
        forbidden_tls = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Forbidden)
        forbidden_tls.daemon_threads = True
        forbidden_tls.socket = tls.wrap_socket(forbidden_tls.socket, server_side=True)
        forbidden_tls_thread = threading.Thread(target=forbidden_tls.serve_forever, daemon=True)
        forbidden_tls_thread.start()
        ports = {name: free_port() for name in ("mysql", "postgres", "flight_sql", "spark_connect")}
        assert len(set(ports.values())) == 4
        aliases = []
        for name, host, ca in (("valid", "localhost", cert), ("untrusted", "localhost", wrong_ca),
                               ("wronghost", "127.0.0.1", cert)):
            aliases.append({"alias": name, "access_key_env": "OTX_TEST_S3_ACCESS_KEY",
                            "secret_key_env": "OTX_TEST_S3_SECRET_KEY", "region": "us-east-1",
                            "endpoint": f"{host}:{storage.server_port}", "ca_file": str(ca)})
        # No scheme override: custom endpoints must retain the HTTPS default.
        config = {"service": {name: {"host": "127.0.0.1", "port": port} for name, port in ports.items()},
                  "connections": {"s3": aliases}}
        config["service"]["security"] = {"mode": "trusted_private"}
        config_file = root / "config.json"
        config_file.write_text(json.dumps(config))
        environment = dict(os.environ, OTX_TEST_S3_ACCESS_KEY=secrets.token_hex(10),
                           OTX_TEST_S3_SECRET_KEY=secrets.token_urlsafe(32))
        log_file = root / "server.log"
        process = None
        def connect():
            return psycopg.connect(host="127.0.0.1", port=ports["postgres"], user="test", dbname="test",
                                   autocommit=True, connect_timeout=2, sslmode="disable")

        def expect_rejected(sql):
            with connect() as client:
                try:
                    client.execute(sql)
                except psycopg.Error as error:
                    assert error.sqlstate, "Failure did not come from the server"
                    assert not client.closed, "An S3 operation error closed the SQL session"
                else:
                    raise AssertionError("S3 accepted a forbidden operation")
                assert client.execute("SELECT 42;").fetchone() == (42,)

        try:
            with log_file.open("w") as log:
                process = subprocess.Popen([str(server), "--config", str(config_file), "--data-dir", str(root / "data")],
                                           env=environment, stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None:
                    raise AssertionError("Server exited: " + log_file.read_text()[-2000:])
                try:
                    connection = connect()
                    break
                except psycopg.OperationalError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.1)
            with connection:
                connection.execute("CREATE EXTERNAL TABLE tls_test.people WITH (s3_alias = 'valid', location = 's3://bucket/people.csv', format = 'csv');")
                assert connection.execute("SELECT * FROM tls_test.people ORDER BY id;").fetchall() == [(1, "Alice"), (2, "Bob")]
                assert any(method == "GET" for method, _ in requests)
                print("PASS custom S3 endpoint defaults to HTTPS and accepts its configured CA", flush=True)
                for alias in ("untrusted", "wronghost"):
                    before = len(requests)
                    expect_rejected(f"CREATE EXTERNAL TABLE tls_test.{alias} WITH (s3_alias = '{alias}', location = 's3://bucket/people.csv', format = 'csv');")
                    assert len(requests) == before, "A request reached the fixture despite failed TLS validation"
                    print(f"PASS S3 TLS rejects {alias} without an HTTP request", flush=True)
                for scheme in ("http", "https"):
                    path = f"s3://bucket/redirect_{scheme}.csv"
                    for operation, sql in (
                        ("download", f"CREATE EXTERNAL TABLE tls_test.redirected WITH (s3_alias = 'valid', location = '{path}', format = 'csv');"),
                        ("upload", f"COPY (SELECT * FROM tls_test.people) TO '{path}' WITH (s3_alias = 'valid', format = 'csv');"),
                    ):
                        before = len(requests)
                        expect_rejected(sql)
                        methods = {"HEAD", "GET"} if operation == "download" else {"POST", "PUT"}
                        assert any(method in methods and target.split("?", 1)[0] == f"/bucket/redirect_{scheme}.csv"
                                   for method, target in requests[before:]), "Operation never reached the redirect fixture"
                        assert not forbidden_requests, "S3 followed a redirect to an unconfigured destination"
                        assert not connection.execute("SELECT relname FROM pg_catalog.pg_class WHERE relname = 'redirected';").fetchall()
                        print(f"PASS S3 {operation} refuses redirect to {scheme} without contacting another destination", flush=True)
                assert not list((root / "data" / "s3-staging").iterdir()), "S3 staging leaked files"
                print("PASS S3 private staging is cleaned after ingestion and failures", flush=True)
        except BaseException:
            print(f"Server exit: {process.poll() if process else None}; HTTP requests: {requests}; forbidden: {forbidden_requests}", flush=True)
            print(log_file.read_text()[-12000:], flush=True)
            raise
        finally:
            if process is not None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
            storage.shutdown()
            storage.server_close()
            thread.join(timeout=5)
            forbidden.shutdown()
            forbidden.server_close()
            forbidden_thread.join(timeout=5)
            forbidden_tls.shutdown()
            forbidden_tls.server_close()
            forbidden_tls_thread.join(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", type=Path, required=True)
    run(parser.parse_args().server.resolve())
