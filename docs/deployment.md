# Deployment boundaries

Each tenant needs a separate server process, data directory, connector
configuration and network endpoint. The scheduler and catalog do not enforce
permissions between tenants inside one process.

See [transaction semantics and current protocol limits](transactions.md) before
using native transaction control or client pipelines.

## Storage

The storage directory comes from `--data-dir`, then `OTTERSTAX_DATA_DIR`, then
`./data`. It contains engine data, WAL and logs. Docker images set
`OTTERSTAX_DATA_DIR=/app/data`; mount the tenant's persistent volume there.

The process acquires an exclusive OS lock on `.otterstax.lock` before starting
the engine or loggers. Another process using the same directory fails startup.
The OS releases ownership when the process exits, including after a crash.
The lock file remains in place; do not delete or replace it while a server runs.

The engine is built from the pinned recipe in `cmake/conan-recipes/otterbrix`.
Its source patches establish a durable empty table root before write-through
and drain transaction undo after failed DML. The application keeps ingested rows
and partition checkpoints in one engine transaction. Kafka recovery errors
prevent listeners from opening; an absent Kafka registry is a normal first start.

Arrow is built from the pinned recipe in `cmake/conan-recipes/arrow`; its S3 client
disables HTTP redirects for explicit endpoints.

## Listeners

MySQL and PostgreSQL accept `service.mysql.host` and `service.postgres.host`
as IPv4 or IPv6 addresses. Their default is `0.0.0.0`. Flight SQL and Spark
Connect use their respective `service.flight_sql.host` and
`service.spark_connect.host` settings. Configure each tenant's listeners and
network access separately.

## Client identity

`service.security.mode` is required at startup. `trusted_private` admits clients
without checking credentials and requires an access-controlled network. The
checked-in demo, benchmark and test configurations explicitly select this mode.
It can use TLS by configuring both `certificate_file` and `private_key_file`.

`keycloak` requires those TLS files and a confidential Keycloak client. Each
tenant instance needs its own client audience. Configure the audience mapper in
Keycloak to include that client ID in access tokens for authorized clients.

```yaml
service:
  security:
    mode: keycloak
    certificate_file: /run/tls/server.crt
    private_key_file: /run/tls/server.key
    keycloak:
      issuer: https://identity.example/realms/tenant
      client_id: tenant-database
      client_secret_env: OTTERSTAX_KEYCLOAK_SECRET
      ca_file: /run/tls/identity-ca.crt
      request_timeout_ms: 5000
      max_token_bytes: 16384
      max_response_bytes: 65536
      max_in_flight: 16
    files:
      read_roots: [/srv/imports]
      write_roots: [/srv/exports]
      max_file_bytes: 67108864
```

The secret is read from the named environment variable, never from a literal
YAML credential. `ca_file` is optional when the provider uses the system trust
store. The issuer must be an HTTPS realm URL without a trailing slash. The
server calls its Keycloak token introspection endpoint over verified HTTPS,
with no redirects or positive-result cache. It requires an active Bearer access
token, exact issuer, the instance's audience, a subject and a future expiration;
future `nbf` values are refused. Invalid tokens and provider failures deny access.

Clients acquire access tokens from Keycloak themselves. The database does not
exchange user passwords for tokens:

- Flight SQL and Spark Connect send `authorization: Bearer <access-token>` on
  every RPC over TLS, including ticket, session and data operations.
- MySQL negotiates TLS, then responds to the `mysql_clear_password` authentication
  switch with the access token. The client must enable that standard plugin and
  verify the server certificate and hostname.
- PostgreSQL negotiates TLS and supplies the access token as its password in the
  standard PasswordMessage exchange. Use `sslmode=verify-full` with the appropriate
  trust root. This is token authentication through PasswordMessage, not SASL
  OAUTHBEARER or a Keycloak password grant.

Native connections revalidate their token before each command/message; reconnect
with a fresh token after expiry or revocation. Each gRPC call validates its own
metadata. An accepted operation may finish after its token expires. Native
introspection occupies one connection executor thread until the configured HTTP
timeout; the provider concurrency cap applies across all four frontends.

An accepted identity has access to the entire tenant instance, its catalog and
configured backends. There are no per-user SQL roles or table grants. User-specified
Kafka destinations still require an authorization boundary before admitting
untrusted tenant operators.

## SQL file permissions

In Keycloak mode, local imports and exports require `service.security.files`
permissions. Omitted or empty `read_roots` and `write_roots` deny that operation.
Roots must be existing absolute directories, with no symbolic-link components.
The server refuses roots that overlap its data directory, configuration file or
configured TLS files. Give each tenant separate import/export directories owned
by its server account; untrusted OS users must not be able to rename their entries.
Do not place credentials or other private files inside an allowed root.

Imports use descriptors opened relative to the configured directories. Parent
traversal, symbolic links, hard links and non-regular files are refused. The
converter receives bytes from that descriptor rather than reopening a checked
pathname. `max_file_bytes` bounds input bytes and output bytes, with a default of
64 MiB. It is not a query-memory or decompressed-Parquet-memory quota.

Exports write a private file in the authorized destination directory, then sync
and publish it atomically. A conversion or size-limit failure preserves an
existing destination and removes the unfinished output. Published files have
mode `0600`. A directory-sync failure is reported even if publication has occurred.
Temporary exports use random names and never replace an existing destination.

Trusted-private mode permits all local directories; file type checks, atomic
exports and the configured size bound still apply. It is suitable only for trusted
SQL clients. S3 uses a separate internal file manager and a private `s3-staging`
directory under the tenant data directory, with the same size bound. SQL clients
cannot use this internal manager to evade their local file permissions.

## S3 transport and credentials

S3 connections require an explicit `endpoint`, including on AWS, and use HTTPS
by default. An endpoint is a
hostname with an optional port; configure its transport separately as `scheme`.
TLS always verifies the certificate chain and hostname. `ca_file` supplies a PEM
trust bundle for private object stores; otherwise the TLS library's trust store
applies. Local HTTP object-store fixtures explicitly set `scheme: http`.
For AWS, configure the S3 regional endpoint (for example,
`s3.us-east-1.amazonaws.com`) and its matching `region`. Requests use path-style
bucket addressing. SQL selects a configured alias and a bucket/object path;
URLs, authorities and access-point ARNs are rejected before creating a client.
HTTP redirects are rejected, including redirects to a different scheme or port.
Configure the destination region's endpoint directly.

```yaml
connections:
  s3:
    - alias: storage
      access_key_env: OTTERSTAX_S3_ACCESS_KEY
      secret_key_env: OTTERSTAX_S3_SECRET_KEY
      region: us-east-1
      endpoint: storage.example:9443
      scheme: https
      ca_file: /run/tls/storage-ca.crt
```

Use `session_token_env` when temporary credentials require a session token. Each
environment reference must name a populated variable and cannot coexist with its
inline field. The process resolves these references at startup. Authentication
failures and TLS failures surface as operation errors; no plaintext fallback or
certificate-verification bypass is available.

## Kafka destinations

Both deployment modes require every Kafka destination in
`service.security.kafka_brokers`. Missing or empty lists deny Kafka connections.
Entries are exact `hostname:port`, `IPv4:port` or `[IPv6]:port` values; wildcards,
URL schemes, credentials and implicit ports are rejected at startup.

```yaml
service:
  security:
    mode: trusted_private
    kafka_brokers: ["broker-a.example:9092", "broker-b.example:9092"]
```

SQL `BOOTSTRAP_SERVERS` must select only listed entries. Include every advertised
broker, not just the bootstrap address: producers and consumers apply the same
policy before resolving discovered brokers. A hostname authorizes its DNS
resolution; use administrator-controlled DNS. Hostnames are case-insensitive,
IPv6 spelling is normalized, and ports are numeric. IP literals and DNS aliases
need separate entries. Persisted objects are revalidated on startup; removing a
destination still referenced by an object prevents successful recovery.

`tests/test_client_security.py` exercises the actual server protocols against an
HTTPS introspection fixture. `tests/test_keycloak_live.py` owns a separate local
Keycloak container for live signature, audience and expiry checks. Both generate
temporary credentials and certificates and require an explicitly supplied server
binary; neither targets an existing production identity provider.
