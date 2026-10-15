"""Validate client access with an isolated real Keycloak container.

Run inside the integration client image with the Docker socket available, on
the network passed by --network. The test owns and removes its Keycloak
container. It never contacts or modifies an existing identity provider.
"""

import argparse
import base64
import contextlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

import grpc

from test_client_security import (
    expect_denied, free_port, mysql_connect, mysql_ping, pg_connect, pg_query, rpc_status,
)


def run(server, network, keycloak_image):
    container_name = "otterstax-keycloak-test-" + secrets.token_hex(6)
    environment = dict(os.environ, OTX_KC_CLIENT_SECRET=secrets.token_urlsafe(32),
                       OTX_KC_OTHER_SECRET=secrets.token_urlsafe(32))

    def docker(*args):
        return subprocess.run(["docker", *args], env=environment, check=True, capture_output=True, text=True).stdout

    with tempfile.TemporaryDirectory(prefix="otterstax-live-identity-") as directory:
        root = Path(directory)
        certificate, private_key = root / "cert.pem", root / "key.pem"
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(private_key), "-out", str(certificate), "-subj", f"/CN={container_name}",
            "-addext", f"subjectAltName=DNS:{container_name},DNS:localhost,IP:127.0.0.1",
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # The key is an ephemeral test key; the non-root Keycloak container owns
        # its copy. No private key or client secret is embedded in test sources.
        private_key.chmod(0o600)
        os.chown(private_key, 1000, 0)
        clients = []
        for client_id, variable in (("tenant-database", "OTX_KC_CLIENT_SECRET"), ("other-tenant", "OTX_KC_OTHER_SECRET")):
            clients.append({
                "clientId": client_id, "secret": "${" + variable + "}", "enabled": True,
                "protocol": "openid-connect", "publicClient": False, "serviceAccountsEnabled": True,
                "standardFlowEnabled": False, "directAccessGrantsEnabled": False,
                "protocolMappers": [{"name": "audience", "protocol": "openid-connect",
                    "protocolMapper": "oidc-audience-mapper", "config": {
                        "included.client.audience": client_id, "access.token.claim": "true"}}],
            })
        (root / "import").mkdir()
        realm = root / "import" / "otx-test-realm.json"
        realm.write_text(json.dumps({"realm": "otx-test", "enabled": True, "accessTokenLifespan": 5, "clients": clients}))
        docker("create", "--name", container_name, "--network", network, "--network-alias", container_name,
               "--cpus", "1", "--memory", "1536m", "-e", "OTX_KC_CLIENT_SECRET", "-e", "OTX_KC_OTHER_SECRET",
               keycloak_image, "start-dev", "--import-realm", "--http-enabled=false",
               "--hostname=https://" + container_name + ":8443", "--https-certificate-file=/tmp/cert.pem",
               "--https-certificate-key-file=/tmp/key.pem")
        process = None
        channels = []
        try:
            docker("cp", str(certificate), container_name + ":/tmp/cert.pem")
            docker("cp", "-a", str(private_key), container_name + ":/tmp/key.pem")
            docker("cp", str(root / "import"), container_name + ":/opt/keycloak/data/")
            docker("start", container_name)
            tls = ssl.create_default_context(cafile=str(certificate))
            issuer = f"https://{container_name}:8443/realms/otx-test"
            deadline = time.monotonic() + 120
            while True:
                try:
                    with urllib.request.urlopen(issuer + "/.well-known/openid-configuration", context=tls, timeout=2) as response:
                        discovery = json.load(response)
                        assert discovery["issuer"] == issuer
                        break
                except (urllib.error.URLError, OSError):
                    if time.monotonic() >= deadline:
                        raise AssertionError("Isolated Keycloak failed startup: " + docker("logs", "--tail", "25", container_name))
                    time.sleep(0.5)
            print("PASS Keycloak: realm import and verified HTTPS discovery", flush=True)

            def issue(client="tenant-database", variable="OTX_KC_CLIENT_SECRET"):
                data = urllib.parse.urlencode({"grant_type": "client_credentials", "client_id": client,
                                               "client_secret": environment[variable]}).encode()
                with urllib.request.urlopen(issuer + "/protocol/openid-connect/token", data=data, context=tls, timeout=10) as response:
                    return json.load(response)["access_token"]

            ports = {name: free_port() for name in ("mysql", "postgres", "flight_sql", "spark_connect")}
            assert len(set(ports.values())) == 4
            config = {"service": {name: {"host": "127.0.0.1", "port": port} for name, port in ports.items()}}
            config["service"]["security"] = {
                "mode": "keycloak", "certificate_file": str(certificate), "private_key_file": str(private_key),
                "keycloak": {"issuer": issuer, "client_id": "tenant-database", "client_secret_env": "OTX_KC_CLIENT_SECRET",
                             "ca_file": str(certificate), "request_timeout_ms": 3000},
            }
            config_file = root / "config.json"
            config_file.write_text(json.dumps(config))
            log_file = root / "server.log"
            with log_file.open("w") as log:
                process = subprocess.Popen([str(server), "--config", str(config_file), "--data-dir", str(root / "data")],
                                           stdout=log, stderr=subprocess.STDOUT, env=environment)
            deadline = time.monotonic() + 45
            while True:
                if process.poll() is not None:
                    raise AssertionError("Database startup failed: " + log_file.read_text()[-1500:])
                try:
                    with socket.create_connection(("127.0.0.1", ports["flight_sql"]), timeout=0.5):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Database startup timeout")
                    time.sleep(0.1)
            for name, connect, command in (("postgres", pg_connect, pg_query), ("mysql", mysql_connect, mysql_ping)):
                with connect(ports[name], tls, issue()) as sock:
                    command(sock)
                expect_denied(lambda: connect(ports[name], tls, issue("other-tenant", "OTX_KC_OTHER_SECRET")))
                token = issue()
                parts = token.split(".")
                altered = parts[0] + "." + parts[1] + "." + ("A" if parts[2][0] != "A" else "B") + parts[2][1:]
                expect_denied(lambda: connect(ports[name], tls, altered))
                unsigned = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode() + "." + parts[1] + "."
                expect_denied(lambda: connect(ports[name], tls, unsigned))
                with connect(ports[name], tls, issue()) as sock:
                    time.sleep(6)
                    expect_denied(lambda: command(sock))
                print(f"PASS Keycloak/{name}: valid, wrong tenant, forged, unsigned, expired session", flush=True)

            credentials = grpc.ssl_channel_credentials(certificate.read_bytes())
            for name, service, method, shape in (
                ("flight_sql", "arrow.flight.protocol.FlightService", "ListActions", "unary_stream"),
                ("spark_connect", "spark.connect.SparkConnectService", "Config", "unary_unary"),
            ):
                channel = grpc.secure_channel(f"localhost:{ports[name]}", credentials)
                channels.append(channel)
                grpc.channel_ready_future(channel).result(timeout=10)
                status = rpc_status(channel, service, method, shape, (("authorization", "Bearer " + issue()),))
                # An empty Spark Config request has no operation; its handler
                # rejects the request after successful authentication.
                expected = grpc.StatusCode.OK if name == "flight_sql" else grpc.StatusCode.INVALID_ARGUMENT
                assert status == expected, (name, status)
                wrong_tenant = issue("other-tenant", "OTX_KC_OTHER_SECRET")
                assert rpc_status(channel, service, method, shape, (("authorization", "Bearer " + wrong_tenant),)) == grpc.StatusCode.UNAUTHENTICATED
                print(f"PASS Keycloak/{name}: valid identity reaches handler; other tenant refused", flush=True)
            assert environment["OTX_KC_CLIENT_SECRET"] not in log_file.read_text()
        finally:
            for channel in channels:
                channel.close()
            if process is not None and process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError("Database shutdown timeout")
            docker("rm", "-f", container_name)
        assert process is not None and process.returncode == 0
        print("Live Keycloak checks passed", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--keycloak-image", required=True)
    args = parser.parse_args()
    run(args.server.resolve(), args.network, args.keycloak_image)
