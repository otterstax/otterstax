# Client security

`access` validates Keycloak access tokens using bounded HTTPS introspection.
The provider verifies signing keys and sessions; this server also requires the
configured issuer/audience, Bearer token type, subject, expiration and optional
not-before claim. No positive results are cached. Secrets come from an environment
variable named in configuration. Never log tokens, secrets, form bodies or provider
responses.

`transport` builds TLS and gRPC credentials before the engine/listeners start.
Missing deployment mode fails startup. Keycloak mode requires TLS; trusted-private
mode explicitly admits all network clients. Each gRPC call is checked by the
credentials' AuthMetadataProcessor. Its AuthContext is connection-shared and must
not store per-call identity or tokens.

The native frontends upgrade their existing socket after the protocol SSL request.
All later reads and writes use the TLS stream. Every callback, including upgrade
and post-write continuations, retains the existing connection lifetime accounting.
MySQL uses mysql_clear_password; PostgreSQL uses PasswordMessage. These carry
pre-acquired access tokens inside TLS. Both check the token again before new commands.

See `docs/deployment.md` for configuration, authorization boundaries and client
requirements. Claim parser/config tests live in `tests/unit/config`; real protocol
and live-provider tests are `tests/test_client_security.py` and
`tests/test_keycloak_live.py`.

`file_access` holds POSIX directory descriptors for configured read/write roots.
Every path component is opened with `openat`/`O_NOFOLLOW`; never replace this with
canonical-path validation followed by a pathname open. Imports reject hard links
and special files, then pass bounded bytes to converters. `file_output` is an
Arrow stream over an exclusively created private file. Only `commit()` publishes
it; destruction discards incomplete output. Keep configured directories under
the deployment account's control. The descriptor boundary does not defend against
an OS user who can rename entries in those directories.

The executable constructs the SQL policy before the engine. ComponentManager
gives S3 a separate file actor restricted to private tenant staging. Do not pass
the S3 actor's file permissions to SQL or add a client-controlled bypass flag.
File limits bound encoded file bytes, not total query or decompression memory.

`network_destinations` matches exact canonical host/port entries, denies empty
lists and refuses wildcard, URL and ambiguous numeric-address syntax. Kafka
uses this policy for SQL bootstrap lists and every resolver call, including
advertised brokers. The librdkafka C++ API owns its opaque pointer; the C
thread-start interceptor installs each handle's policy in its private thread
context before DNS resolution. Never overwrite the library's opaque pointer or
permit a resolver call without its policy. S3 endpoints come only from startup
aliases; clients use path-style bucket addressing and reject ARN/authority paths.
