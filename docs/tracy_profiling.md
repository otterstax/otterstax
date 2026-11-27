# Tracy Profiling

OtterStax integrates the [Tracy](https://github.com/wolfpld/tracy) frame profiler.
All instrumentation is zero-overhead when Tracy is disabled — the macros compile to nothing.

## Requirements

- Tracy GUI / capture CLI **v0.13.1** — must match the library version exactly.
  Download from [github.com/wolfpld/tracy/releases/tag/v0.13.1](https://github.com/wolfpld/tracy/releases/tag/v0.13.1)
- Tracy uses TCP port **8086** (server listens, GUI/capture connects).

## Macro reference

All instrumentation uses the `OTX_` wrappers defined in `utility/tracy_profiler.hpp`.

| Macro | Purpose |
| --- | --- |
| `OTX_ZONE()` | Zone covering the current scope (uses function name) |
| `OTX_ZONE_N("name")` | Zone with an explicit name |
| `OTX_FRAME()` | Mark end of a frame (unnamed) |
| `OTX_FRAME_N("name")` | Mark end of a named frame |
| `OTX_PLOT("name", value)` | Plot a numeric value |
| `OTX_MESSAGE_L("literal")` | Emit a string literal message |
| `OTX_MESSAGE(str)` | Emit a runtime string message |

## Building with Tracy

### devcontainer / local build

```bash
conan install conanfile.py --build missing -s build_type=Release -s compiler.cppstd=20 \
  -o "&:with_tracy=True"
cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake
cmake --build build/Release --target server --parallel <JOBS>   # size <JOBS> to RAM (root CLAUDE.md)
```

Verify Tracy symbols are present:

```bash
nm build/Release/server | grep -i tracy | head -5
```

### Docker (main image)

Resolve the release dependency image before building with Tracy:

```bash
./scripts/deps-image.sh release
WITH_TRACY=true docker compose up --build
```

The default Dockerfile target is the runtime image. Use the host capture CLI
with that image, or build the `builder` target to get `tracy-capture`:

```bash
docker build --target builder --build-arg DEPS_IMAGE="$(./scripts/deps-image.sh release)" \
  --build-arg WITH_TRACY=true -t otterstax:tracy-builder .
```

### Integration test image

```bash
DEPS_IMAGE="$(./scripts/deps-image.sh release)" WITH_TRACY=true \
  docker compose -f compose.test.yml build test-otterstax
```

## Connecting the Tracy GUI

1. Start the server (it listens on `*:8086` automatically when built with Tracy).
2. Open the Tracy GUI (`tracy-profiler` on Linux/Mac, `Tracy.exe` on Windows).
3. Click **Connect** and enter `localhost` / port `8086`.

**devcontainer users:** VS Code must forward port 8086 to your host.

- Open the **Ports** panel → click **Forward a Port** → enter `8086`.
- `forwardPorts: [8086]` in `.devcontainer/devcontainer.json` does this automatically on container rebuild.

## Saving a trace without the GUI

Use `tracy-capture` (headless CLI, same version as the library):

```bash
# Capture for 60 seconds then stop
tracy-capture -a localhost -p 8086 -o trace.tracy -s 60

# Capture until Ctrl+C
tracy-capture -a localhost -p 8086 -o trace.tracy -f
```

Open the saved file later in the GUI: **File → Open → trace.tracy**

## Integration tests — `docker-run-tests.sh`

### Basic usage

```bash
# Run the full integration test suite (no Tracy)
./docker-run-tests.sh

# Run with Tracy — builds server with Tracy, auto-captures a trace
./docker-run-tests.sh --tracy
```

### What the script does (step by step)

| Step | Action |
| --- | --- |
| 1 | Removes previous containers and volumes in the `otterstax-tests` project, then rebuilds the test images |
| 2–3 | Starts the databases and stores (MariaDB ×2, PostgreSQL, ClickHouse, redpanda for Kafka, RustFS), then waits for each to be healthy |
| 4–6 | Creates the test data, verifies it, checks specific tables |
| 7 | Starts `test-otterstax`, waits for its FlightSQL wire port (8815) to accept connections |
| 7b | *(`--tracy`)* starts the `tracy-capture` sidecar in the test network |
| 8 | Runs the main test suite inside `test-client` |
| 8a | FlightSQL end-to-end against the original drivers |
| 8c | Kafka crash-recovery (exactly-once through `kill -9`) — plain suite only, skipped under Tracy / ASAN / TSAN |
| 8d | Spark Connect client matrix (20 PySpark versions) |
| 8b | *(`--tracy`)* stops OtterStax first and waits for the capture to write its profile |
| 9 | Tears everything down again |

### Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `WAIT_RETRIES` | `120` | How many readiness poll attempts before giving up |
| `WAIT_SLEEP` | `2` | Seconds between each poll attempt |
| `IMAGE_TAG` | *(unset)* | Skip rebuilding `test-otterstax` and use a pre-built image (CI / sanitizer path) |
| `WITH_TRACY` | `false` | Set by the script when `--tracy` is passed; can also be set manually |

```bash
# Faster polling (60 retries × 1 s = 60 s max wait)
WAIT_RETRIES=60 WAIT_SLEEP=1 ./docker-run-tests.sh

# Use a pre-built image (e.g. from a CI ASAN workflow)
IMAGE_TAG=asan ./docker-run-tests.sh
```

### How `--tracy` works inside `docker-run-tests.sh`

Understanding each step helps when things go wrong or when you want a different workflow.

#### Flag parsing (top of script)

```text
--tracy  →  ENABLE_TRACY=true
            TRACY_OUTPUT_DIR=<repo>/tracy_profiles/YYYYMMDD_HHMMSS/
            TRACY_FILE=<TRACY_OUTPUT_DIR>/otterstax.tracy
            mkdir -p <TRACY_OUTPUT_DIR>
```

A shell trap handles `EXIT`, `INT` and `TERM`. If a capture is active, it
stops OtterStax, allows up to 10 seconds for the capture to finish, and removes
the capture container. It reports the profile path if the file was written.

#### Build step

`ENABLE_TRACY=true` causes the script to `export WITH_TRACY=true` before
calling `docker compose build`. The runner resolves the dependency image when
it builds the test server. Docker Compose reads `WITH_TRACY` as the
`WITH_TRACY` build arg (declared in `compose.test.yml`), which triggers:

1. `conan install` with `-o "&:with_tracy=True"` → fetches `tracy/0.13.1`
2. CMake toolchain sets `ENABLE_TRACY=ON` → compiles in Tracy zones
3. `tracy-capture` binary is built and copied to `/usr/local/bin/` inside the image

#### Capture step (Step 7b — after otterstax is healthy)

The script runs the image's `tracy-capture` binary in a background container on
the `otterstax-tests_test_app_network` network. The capture connects to
`test-otterstax:8086` and writes `otterstax.tracy` into the run's mounted
profile directory. If the capture container exits during the first two seconds,
the script prints its logs and continues the tests.

#### Stop step (Step 8b — after tests finish)

The script stops OtterStax first. The resulting TCP disconnect lets
`tracy-capture` finalize the profile. The script waits up to 30 seconds before
stopping a capture container that is still running, then prints the capture logs
and reports the file if it exists. A direct `SIGTERM` can interrupt profile
writing.

### Live view without saving a file

To watch a live flame graph as traffic flows through the server, skip `--tracy`
and start the server directly:

```bash
# Terminal 1 — build and run with Tracy but no auto-capture
DEPS_IMAGE="$(./scripts/deps-image.sh release)" WITH_TRACY=true \
  docker compose -f compose.test.yml up --build test-otterstax

# Terminal 2 — open Tracy GUI and connect to localhost:8086
```

The GUI shows zones in real time as queries pass through the server.

### Both live view and a saved file

`tracy-capture` and the GUI can be connected simultaneously — Tracy supports
multiple readers. Run `--tracy` (auto-capture starts in the background) and
also open the GUI and connect to `localhost:8086` at the same time.

### Saving a trace without running tests

If you just want to capture a trace against a running server (no test orchestration):

```bash
# 1. Start the stack with Tracy
./scripts/deps-image.sh release
WITH_TRACY=true docker compose up --build

# 2. In a separate terminal, capture for N seconds
tracy-capture -a localhost -p 8086 -o my_trace.tracy -s 60

# 3. Or capture until you press Ctrl+C
tracy-capture -a localhost -p 8086 -o my_trace.tracy -f
```

The `-f` flag is required when the output file already exists (overwrites it).
Omit `-s` to capture indefinitely.

You can also run just the integration test server and capture independently:

```bash
# Terminal 1 — start a previously built Tracy test server (and its DB dependencies)
docker compose -f compose.test.yml up test-otterstax

# Terminal 2 — capture while you drive traffic manually or run specific tests
tracy-capture -a localhost -p 8086 -o focused.tracy -f
```

## RAM usage

Tracy uses `TRACY_ON_DEMAND` — it only buffers profiling data while a client
(GUI or `tracy-capture`) is connected. Disconnecting frees the buffer immediately,
so leaving the server running without a connected client has no memory cost.

## Troubleshooting

### GUI says "Unable to connect"

- Confirm the server was built with Tracy: `nm build/Release/server | grep -i tracy`
- Confirm the server is listening: `ss -tlnp | grep 8086`
- Confirm the port is forwarded (devcontainer) or mapped (Docker): check the Ports panel or `docker ps`
- GUI and library versions must match exactly (both must be v0.13.1)

### `tracy/Tracy.hpp: No such file or directory` at build time

Re-run `conan install` with `-o "&:with_tracy=True"` before CMake configure.

### `ERROR: Package 'tracy/0.13.0' not resolved`

ConanCenter only carries v0.13.1+. The `conanfile.py` already uses `tracy/0.13.1`.
