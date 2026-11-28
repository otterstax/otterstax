# The system packages, conan and every conan package come prebuilt in the
# dependency image (Dockerfile.deps, target release): this file compiles only
# OtterStax. `./scripts/deps-image.sh release` pulls or builds it and tags it
# otterstax-deps:local; CI passes the published reference.
ARG DEPS_IMAGE=otterstax-deps:local
FROM ${DEPS_IMAGE} AS builder

ARG WITH_TRACY=false
# Parallel jobs for the otterstax build and for any conan package the
# dependency image lacks. otterbrix needs ~1.5-2 GB per job — see CLAUDE.md
# "Build resources" before raising it. 0 = min(nproc, MemTotal_MB / 1536).
ARG BUILD_JOBS=5

# Run conan install from the project root so self.source_folder=/app and
# cmake_layout places generators + libs at build/Release/ — matching the
# devcontainer preset layout exactly. Over the dependency image every package
# is already in conan's cache, so this only writes the generators; a package the
# image lacks (conanfile.py changed since it was built, Tracy) is built here.
WORKDIR /app
COPY conanfile.py .
# A recipe's source download (github.com) can meet a run of 504s (CI): retry
# 10 times, 15 s apart (tools.files.download, default 2 x 5 s).
RUN _jobs="${BUILD_JOBS}" && \
    if [ "$_jobs" = "0" ]; then \
        _mem_jobs=$(( $(awk '/^MemTotal:/ {print int($2/1024)}' /proc/meminfo) / 1536 )) && \
        if [ "$_mem_jobs" -lt 1 ]; then _mem_jobs=1; fi && \
        _jobs=$(( $(nproc) < _mem_jobs ? $(nproc) : _mem_jobs )); \
    fi && \
    echo "conan from-source build jobs: ${_jobs}" && \
    _jobs_conf=(-c "tools.build:jobs=${_jobs}" \
        -c tools.files.download:retry=10 -c tools.files.download:retry_wait=15) && \
    if [ "$WITH_TRACY" = "true" ]; then \
        conan install conanfile.py --build missing "${_jobs_conf[@]}" \
            -s build_type=Release \
            -s compiler.cppstd=gnu20 \
            -o "&:with_tracy=True"; \
    else \
        conan install conanfile.py --build missing "${_jobs_conf[@]}" \
            -s build_type=Release \
            -s compiler.cppstd=gnu20; \
    fi

# Build tracy-capture CLI (headless, no GUI deps) when Tracy is enabled
RUN if [ "$WITH_TRACY" = "true" ]; then \
        curl -L https://github.com/wolfpld/tracy/archive/refs/tags/v0.13.1.tar.gz \
            | tar xz -C /tmp && \
        cmake -S /tmp/tracy-0.13.1/capture -B /tmp/tracy-capture-build \
            -DCMAKE_BUILD_TYPE=Release && \
        cmake --build /tmp/tracy-capture-build -j$(nproc) && \
        cp /tmp/tracy-capture-build/tracy-capture /usr/local/bin/ && \
        rm -rf /tmp/tracy-0.13.1 /tmp/tracy-capture-build; \
    fi

COPY ./catalog ./catalog
COPY ./config ./config
COPY ./connectors ./connectors
COPY ./integration ./integration
COPY ./frontend/ ./frontend
COPY ./otterbrix ./otterbrix
COPY ./runtime ./runtime
COPY ./backends ./backends
COPY ./session ./session
COPY ./types ./types
COPY ./utility ./utility
COPY ./cmake ./cmake
COPY ./main.cpp ./main.cpp
COPY ./CMakeLists.txt ./CMakeLists.txt

# The log level compiled in (CMakeLists.txt's own default is ERROR): at INFO the
# container log shows the startup, the config file read and the stop.
ARG SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO

RUN _jobs="${BUILD_JOBS}" && \
    if [ "$_jobs" = "0" ]; then \
        _mem_jobs=$(( $(awk '/^MemTotal:/ {print int($2/1024)}' /proc/meminfo) / 1536 )) && \
        if [ "$_mem_jobs" -lt 1 ]; then _mem_jobs=1; fi && \
        _jobs=$(( $(nproc) < _mem_jobs ? $(nproc) : _mem_jobs )); \
    fi && \
    cmake -S . -B build/Release \
        -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
        -DSPDLOG_ACTIVE_LEVEL="${SPDLOG_ACTIVE_LEVEL}" \
        -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build/Release --target all -- -j${_jobs}

# Tracy profiler port (active only when built with --build-arg WITH_TRACY=true)
EXPOSE 8086

WORKDIR /app/build/Release
CMD [ "./server" ]

# The published image (the default target): the server, the shared libraries it
# loads from lib/ and a config that names only the data directory. No compiler,
# no sources. The config is /otterstax/config.yaml — bind-mount your own over it
# (it must set service.engine.data_dir too); the data is the volume
# /otterstax/data. The server stops on SIGTERM: give it time to drain the
# sessions and write the final CHECKPOINT (`docker stop -t 60`).
FROM ubuntu:22.04 AS runtime

# libquadmath0: the server links libquadmath.so.0 on amd64 (gcc's __float128
# runtime), which the conan packages in lib/ do not carry.
RUN apt update && \
    DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
        ca-certificates tzdata libquadmath0 && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/Release/server /app/build/Release/server
COPY --from=builder /app/build/Release/lib /app/build/Release/lib

RUN mkdir -p /otterstax/data && \
    printf 'service:\n  engine:\n    data_dir: /otterstax/data\n' > /otterstax/config.yaml

VOLUME /otterstax/data

# Flight SQL, MySQL wire, PostgreSQL wire, Spark Connect.
EXPOSE 8815 8816 8817 15002

WORKDIR /app/build/Release
CMD ["./server", "--config", "/otterstax/config.yaml"]
