"""Run real protocol/TLS checks against an owned server and HTTPS introspection fixture.

Usage: python test_client_security.py --server /path/to/server
Requires grpcio and openssl. All credentials, certificates, ports and storage are
temporary. The fixture exercises transport failures; it does not replace a live
Keycloak deployment acceptance test.
"""

import argparse
import concurrent.futures
import contextlib
import http.server
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
from urllib.parse import parse_qs

import grpc


class AuthenticationDenied(Exception):
    pass


def receive(sock, count):
    data = b""
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise EOFError("Peer closed the connection")
        data += part
    return data


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def pg_message(sock):
    kind = receive(sock, 1)
    length = struct.unpack("!I", receive(sock, 4))[0]
    assert 4 <= length <= 1024 * 1024
    return kind, receive(sock, length - 4)


def pg_send(sock, kind, body):
    sock.sendall(kind + struct.pack("!I", len(body) + 4) + body)


def pg_connect(port, tls, token, encrypt=True):
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    try:
        if encrypt:
            sock.sendall(struct.pack("!II", 8, 80877103))
            assert receive(sock, 1) == b"S"
            sock = tls.wrap_socket(sock, server_hostname="localhost")
        startup = struct.pack("!I", 196608) + b"user\0client\0database\0test\0\0"
        sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
        while True:
            kind, body = pg_message(sock)
            if kind == b"E":
                raise AuthenticationDenied("PostgreSQL rejected authentication")
            if kind == b"R" and body == struct.pack("!I", 3):
                pg_send(sock, b"p", token.encode() + b"\0")
            if kind == b"Z":
                return sock
    except BaseException:
        sock.close()
        raise


def pg_query(sock):
    pg_send(sock, b"Q", b"SELECT 1;\0")
    rows = []
    while True:
        kind, body = pg_message(sock)
        if kind == b"E":
            raise AuthenticationDenied("PostgreSQL refused a command after authentication")
        if kind == b"D":
            rows.append(body)
        if kind == b"Z":
            assert rows == [b"\x00\x01\x00\x00\x00\x011"]
            return


def mysql_message(sock):
    header = receive(sock, 4)
    length = int.from_bytes(header[:3], "little")
    assert length <= 1024 * 1024
    return header[3], receive(sock, length)


def mysql_send(sock, sequence, body):
    sock.sendall(len(body).to_bytes(3, "little") + bytes([sequence]) + body)


def mysql_connect(port, tls, token, encrypt=True):
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    try:
        sequence, handshake = mysql_message(sock)
        assert sequence == 0 and handshake[0] == 10
        flags = 512 | 32768 | (1 << 19) | (2048 if encrypt else 0)
        prefix = struct.pack("<IIB", flags, 1024 * 1024, 33) + bytes(23)
        sequence = 1
        if encrypt:
            mysql_send(sock, sequence, prefix)
            sequence += 1
            sock = tls.wrap_socket(sock, server_hostname="localhost")
        mysql_send(sock, sequence, prefix + b"client\0\0mysql_native_password\0")
        sequence, response = mysql_message(sock)
        if response[0] == 255:
            raise AuthenticationDenied("MySQL rejected authentication")
        assert response.startswith(b"\xfemysql_clear_password\0")
        mysql_send(sock, sequence + 1, token.encode() + b"\0")
        _, response = mysql_message(sock)
        if response[0] == 255:
            raise AuthenticationDenied("MySQL rejected the access token")
        assert response[0] == 0
        return sock
    except BaseException:
        sock.close()
        raise


def mysql_ping(sock):
    mysql_send(sock, 0, b"\x0e")
    sequence, response = mysql_message(sock)
    if response[0] == 255:
        raise AuthenticationDenied("MySQL refused a command after authentication")
    assert sequence == 1 and response[0] == 0


def expect_denied(operation):
    try:
        result = operation()
        if hasattr(result, "close"):
            result.close()
    except AuthenticationDenied:
        return
    raise AssertionError("An unauthorized client was admitted")


FLIGHT_METHODS = {
    "Handshake": "stream_stream", "ListFlights": "unary_stream",
    "GetFlightInfo": "unary_unary", "PollFlightInfo": "unary_unary",
    "GetSchema": "unary_unary", "DoGet": "unary_stream", "DoPut": "stream_stream",
    "DoExchange": "stream_stream", "DoAction": "unary_stream", "ListActions": "unary_stream",
}
SPARK_METHODS = {
    "ExecutePlan": "unary_stream", "AnalyzePlan": "unary_unary", "Config": "unary_unary",
    "AddArtifacts": "stream_unary", "ArtifactStatus": "unary_unary", "Interrupt": "unary_unary",
    "ReattachExecute": "unary_stream", "ReleaseExecute": "unary_unary",
    "ReleaseSession": "unary_unary", "FetchErrorDetails": "unary_unary",
}


def rpc_status(channel, service, name, shape, metadata):
    call = getattr(channel, shape)(f"/{service}/{name}")
    request = iter([b""]) if shape.startswith("stream_") else b""
    try:
        response = call(request, metadata=metadata, timeout=10)
        if shape.endswith("_stream"):
            list(response)
        return grpc.StatusCode.OK
    except grpc.RpcError as error:
        return error.code()


def run(server):
    with tempfile.TemporaryDirectory(prefix="otterstax-client-security-") as directory:
        root = Path(directory)
        certificate, private_key = root / "cert.pem", root / "key.pem"
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(private_key), "-out", str(certificate), "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        secret = secrets.token_urlsafe(32)
        token = secrets.token_urlsafe(32)
        state = {"claims": {}, "requests": 0, "delay": 0, "status": 200, "oversize": False}
        lock = threading.Lock()

        class Provider(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                body = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
                assert self.path == "/realms/tenant/protocol/openid-connect/token/introspect"
                assert body.get("client_secret") == [secret]
                assert body.get("client_id") == ["tenant-database"]
                with lock:
                    state["requests"] += 1
                    claims = dict(state["claims"]) if body.get("token") == [token] else {"active": False}
                    delay, status, oversize = state["delay"], state["status"], state["oversize"]
                time.sleep(delay)
                content = b"x" * 65537 if oversize else json.dumps(claims).encode()
                with contextlib.suppress(BrokenPipeError, ssl.SSLError, ConnectionResetError):
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)

        provider = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Provider)
        provider.daemon_threads = True
        provider_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        provider_tls.minimum_version = ssl.TLSVersion.TLSv1_2
        provider_tls.load_cert_chain(certificate, private_key)
        provider.socket = provider_tls.wrap_socket(provider.socket, server_side=True)
        provider_thread = threading.Thread(target=provider.serve_forever, daemon=True)
        provider_thread.start()
        issuer = f"https://localhost:{provider.server_port}/realms/tenant"
        valid = {"active": True, "iss": issuer, "aud": "tenant-database", "sub": "test-subject",
                 "token_type": "Bearer", "exp": int(time.time()) + 3600}
        state["claims"] = valid.copy()
        ports = {name: free_port() for name in ("mysql", "postgres", "flight_sql", "spark_connect")}
        assert len(set(ports.values())) == 4
        imports = root / "imports"
        exports = root / "exports"
        imports.mkdir()
        exports.mkdir()
        (imports / "people.csv").write_text("id,name\n1,Alice\n2,Bob\n")
        (root / "secret.csv").write_text("secret\nprivate\n")
        (imports / "link.csv").symlink_to(root / "secret.csv")
        (imports / "large.csv").write_bytes(b"x" * 65537)
        config = {"service": {name: {"host": "127.0.0.1", "port": port} for name, port in ports.items()}}
        config["service"]["security"] = {
            "mode": "keycloak", "certificate_file": str(certificate), "private_key_file": str(private_key),
            "files": {"read_roots": [str(imports)], "write_roots": [str(exports)], "max_file_bytes": 65536},
            "keycloak": {"issuer": issuer, "client_id": "tenant-database", "client_secret_env": "OTX_TEST_SECRET",
                         "ca_file": str(certificate), "request_timeout_ms": 300, "max_in_flight": 2},
        }
        config_file = root / "config.json"
        config_file.write_text(json.dumps(config))
        environment = dict(os.environ, OTX_TEST_SECRET=secret)
        log_file = root / "server.log"
        with log_file.open("w") as log:
            process = subprocess.Popen([str(server), "--config", str(config_file), "--data-dir", str(root / "data")],
                                       stdout=log, stderr=subprocess.STDOUT, env=environment)
        tls = ssl.create_default_context(cafile=str(certificate))
        channels = []
        passed = 0

        def check(label, action):
            nonlocal passed
            action()
            passed += 1
            print(f"PASS {label}", flush=True)

        try:
            deadline = time.monotonic() + 60
            while True:
                if process.poll() is not None:
                    raise AssertionError("Server exited during security startup: " + log_file.read_text()[-2500:])
                try:
                    with socket.create_connection(("127.0.0.1", ports["flight_sql"]), timeout=0.5):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise TimeoutError("Server did not start")
                    time.sleep(0.1)
            for name, connect, command in (("postgres", pg_connect, pg_query), ("mysql", mysql_connect, mysql_ping)):
                def valid_native():
                    with connect(ports[name], tls, token) as sock:
                        command(sock)
                check(f"{name}: valid token and encrypted command", valid_native)
                check(f"{name}: TLS required", lambda: expect_denied(lambda: connect(ports[name], tls, token, False)))
                check(f"{name}: invalid token", lambda: expect_denied(lambda: connect(ports[name], tls, "invalid-token")))
                with connect(ports[name], tls, token) as sock:
                    state["claims"] = dict(valid, active=False)
                    check(f"{name}: revocation on existing connection", lambda: expect_denied(lambda: command(sock)))
                state["claims"] = valid.copy()

            def original_mysql_client():
                import pymysql
                with pymysql.connect(host="localhost", port=ports["mysql"], user="client", password=token,
                                     ssl=tls, autocommit=True, read_timeout=10, write_timeout=10) as client:
                    with client.cursor() as cursor:
                        cursor.execute("SELECT 1;")
                        assert cursor.fetchall() == ((1,),)

            def original_postgres_client():
                import psycopg
                with psycopg.connect(host="localhost", port=ports["postgres"], user="client", password=token,
                                      dbname="test", sslmode="verify-full", sslrootcert=str(certificate),
                                      autocommit=True, connect_timeout=10) as client:
                    assert client.execute("SELECT 1;").fetchall() == [(1,)]

            def original_flight_client():
                import pyarrow.flight as flight
                with flight.FlightClient(f"grpc+tls://localhost:{ports['flight_sql']}",
                                         tls_root_certs=certificate.read_bytes()) as client:
                    options = flight.FlightCallOptions(headers=[(b"authorization", b"Bearer " + token.encode())])
                    assert list(client.list_actions(options))

            check("PyMySQL: verified TLS and token authentication", original_mysql_client)
            check("psycopg: verified TLS and token authentication", original_postgres_client)
            check("Arrow Flight: verified TLS and bearer metadata", original_flight_client)

            def authorized_file_sql():
                import psycopg
                with psycopg.connect(host="localhost", port=ports["postgres"], user="client", password=token,
                                     dbname="test", sslmode="verify-full", sslrootcert=str(certificate),
                                     autocommit=True, connect_timeout=10) as client:
                    client.execute(f"CREATE EXTERNAL TABLE auth_files.people WITH (location = '{imports / 'people.csv'}', format = 'csv');")
                    assert client.execute("SELECT * FROM auth_files.people ORDER BY id;").fetchall() == [(1, "Alice"), (2, "Bob")]
                    client.execute(f"COPY (SELECT * FROM auth_files.people) TO '{exports / 'people.csv'}' WITH (format = 'csv');")
                    assert "Alice" in (exports / "people.csv").read_text()
                    denied_inputs = [root / "secret.csv", imports / "link.csv", imports / "large.csv",
                                     imports / ".." / "secret.csv", root / "data" / "s3-staging" / "object.csv"]
                    for index, path in enumerate(denied_inputs):
                        try:
                            client.execute(f"CREATE EXTERNAL TABLE auth_files.denied_{index} WITH (location = '{path}', format = 'csv');")
                        except psycopg.Error:
                            pass
                        else:
                            raise AssertionError(f"Unauthorized import succeeded: {path}")
                    for path in [root / "secret.csv", imports / "people.csv", private_key]:
                        before = path.read_bytes()
                        try:
                            client.execute(f"COPY (SELECT * FROM auth_files.people) TO '{path}' WITH (format = 'csv');")
                        except psycopg.Error:
                            pass
                        else:
                            raise AssertionError(f"Unauthorized export succeeded: {path}")
                        assert path.read_bytes() == before

            check("SQL file permissions: allowed import/export, forbidden paths, links, limits and private staging", authorized_file_sql)

            credentials = grpc.ssl_channel_credentials(root_certificates=certificate.read_bytes())
            for name, service, methods in (
                ("flight_sql", "arrow.flight.protocol.FlightService", FLIGHT_METHODS),
                ("spark_connect", "spark.connect.SparkConnectService", SPARK_METHODS),
            ):
                channel = grpc.secure_channel(f"localhost:{ports[name]}", credentials)
                channels.append(channel)
                grpc.channel_ready_future(channel).result(timeout=10)
                for method, shape in methods.items():
                    for metadata in ((), (("authorization", "Bearer invalid-token"),),
                                     (("authorization", "Bearer " + token), ("authorization", "Bearer " + token))):
                        status = rpc_status(channel, service, method, shape, metadata)
                        assert status == grpc.StatusCode.UNAUTHENTICATED, (name, method, status)
                    print(f"PASS {name}/{method}: missing, invalid, duplicate credentials", flush=True)
                    passed += 3
                if name == "flight_sql":
                    assert rpc_status(channel, service, "ListActions", "unary_stream",
                                      (("authorization", "Bearer " + token),)) == grpc.StatusCode.OK
                    passed += 1

            flight_channel = channels[0]
            flight_call = lambda: rpc_status(flight_channel, "arrow.flight.protocol.FlightService", "ListActions",
                                            "unary_stream", (("authorization", "Bearer " + token),))
            for changes in ({"active": False}, {"exp": int(time.time()) - 1}, {"iss": issuer + "-other"},
                            {"aud": "other-tenant"}, {"token_type": "Refresh"}, {"nbf": int(time.time()) + 60}):
                state["claims"] = dict(valid, **changes)
                assert flight_call() == grpc.StatusCode.UNAUTHENTICATED
                for name, connect in (("mysql", mysql_connect), ("postgres", pg_connect)):
                    expect_denied(lambda: connect(ports[name], tls, token))
                passed += 3
            state["claims"] = valid.copy()
            print("PASS all protocols: inactive, expired, wrong issuer/audience, refresh, future tokens", flush=True)
            for field, value in (("delay", 0.6), ("status", 503), ("status", 302), ("oversize", True)):
                state[field] = value
                assert flight_call() == grpc.StatusCode.UNAVAILABLE, field
                expect_denied(lambda: pg_connect(ports["postgres"], tls, token))
                expect_denied(lambda: mysql_connect(ports["mysql"], tls, token))
                state[field] = {"delay": 0, "status": 200, "oversize": False}[field]
                passed += 3
            print("PASS provider failures: timeout, HTTP error, redirect, oversized body", flush=True)

            # One gRPC HTTP/2 connection multiplexes calls; rejecting one caller
            # must not authenticate or contaminate another caller on that connection.
            def mixed_call(index):
                metadata = (("authorization", "Bearer " + (token if index % 2 else "invalid-token")),)
                status = rpc_status(flight_channel, "arrow.flight.protocol.FlightService", "ListActions", "unary_stream", metadata)
                expected = (grpc.StatusCode.OK, grpc.StatusCode.UNAVAILABLE) if index % 2 else (
                    grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.UNAVAILABLE)
                assert status in expected, status
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(mixed_call, range(40)))
            assert flight_call() == grpc.StatusCode.OK, "Concurrent failures leaked provider admission slots"
            passed += 40
            print("PASS concurrent callers on one gRPC connection", flush=True)
            assert token not in log_file.read_text() and secret not in log_file.read_text()
        finally:
            for channel in channels:
                channel.close()
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError("Authenticated server did not shut down cleanly")
            provider.shutdown()
            provider.server_close()
            provider_thread.join(timeout=5)
        assert process.returncode == 0, process.returncode
        print(f"Client security checks: {passed} passed", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    args = parser.parse_args()
    run(args.server.resolve())
