FROM ubuntu:22.04 AS builder

ENV TZ=America/US
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONFAULTHANDLER=1

ARG WITH_TRACY=false
# Parallel jobs for the conan from-source builds (tools.build:jobs) and the
# otterstax build. otterbrix needs ~1.5-2 GB per job — see CLAUDE.md
# "Build resources" before raising it. 0 = min(nproc, MemTotal_MB / 1536).
ARG BUILD_JOBS=5

RUN apt update && \
    apt upgrade -y && \
    apt install -y \
        build-essential \
        ninja-build \
        python3-pip \
        python3-venv \
        python3-dev curl gnupg apt-transport-https \
        zlib1g libgflags2.2 libgflags-dev \
        libssl-dev bison flex && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

# cmake.org answers 403 to conan's HTTP client, so the conancenter cmake binary
# recipe cannot be fetched; the pip-installed cmake is declared as the platform
# tool_requires of the build profile instead (it satisfies the recipes' <4 range).
RUN pip3 install --no-cache-dir conan==2.21.0 'cmake<4.0' && \
     conan profile detect --force && \
     printf '\n[platform_tool_requires]\ncmake/%s\n' "$(cmake --version | head -1 | awk '{print $3}')" \
         >> "$(conan profile path default)" && \
     conan remote add otterbrix https://conan.otterbrix.com

RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# Run conan install from the project root so self.source_folder=/app and
# cmake_layout places generators + libs at build/Release/ — matching the
# devcontainer preset layout exactly.
WORKDIR /app
COPY conanfile.py .
# TEMPORARY (OTB-tag, docs/proposals/otterbrix-embedding.md): restore the
# local-only otterbrix recipe archived by scripts/conan/export_local_packages.sh
# and build it from source; without the archive the install falls back to the
# remotes. actor-zeta comes from the remote like any other dependency. See
# Dockerfile.test.
COPY .conan-local/ /tmp/conan-local/
RUN _local_build=() && \
    if [ -f /tmp/conan-local/local_recipes.tgz ]; then \
        conan cache restore /tmp/conan-local/local_recipes.tgz && \
        _local_build=(--build="otterbrix/*"); \
    fi && \
    rm -rf /tmp/conan-local && \
    _jobs="${BUILD_JOBS}" && \
    if [ "$_jobs" = "0" ]; then \
        _mem_jobs=$(( $(awk '/^MemTotal:/ {print int($2/1024)}' /proc/meminfo) / 1536 )) && \
        if [ "$_mem_jobs" -lt 1 ]; then _mem_jobs=1; fi && \
        _jobs=$(( $(nproc) < _mem_jobs ? $(nproc) : _mem_jobs )); \
    fi && \
    echo "conan from-source build jobs: ${_jobs}" && \
    _jobs_conf=(-c "tools.build:jobs=${_jobs}") && \
    if [ "$WITH_TRACY" = "true" ]; then \
        conan install conanfile.py --build missing "${_local_build[@]}" "${_jobs_conf[@]}" \
            -s build_type=Release \
            -s compiler.cppstd=20 \
            -s 'clickhouse-cpp/*:compiler.cppstd=17' \
            -s 'abseil/*:compiler.cppstd=17' \
            -s 'protobuf/*:compiler.cppstd=17' \
            -s 're2/*:compiler.cppstd=17' \
            -s 'grpc/*:compiler.cppstd=17' \
            -o "&:with_tracy=True"; \
    else \
        conan install conanfile.py --build missing "${_local_build[@]}" "${_jobs_conf[@]}" \
            -s build_type=Release \
            -s compiler.cppstd=20 \
            -s 'clickhouse-cpp/*:compiler.cppstd=17' \
            -s 'abseil/*:compiler.cppstd=17' \
            -s 'protobuf/*:compiler.cppstd=17' \
            -s 're2/*:compiler.cppstd=17' \
            -s 'grpc/*:compiler.cppstd=17'; \
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

RUN _jobs="${BUILD_JOBS}" && \
    if [ "$_jobs" = "0" ]; then \
        _mem_jobs=$(( $(awk '/^MemTotal:/ {print int($2/1024)}' /proc/meminfo) / 1536 )) && \
        if [ "$_mem_jobs" -lt 1 ]; then _mem_jobs=1; fi && \
        _jobs=$(( $(nproc) < _mem_jobs ? $(nproc) : _mem_jobs )); \
    fi && \
    cmake -S . -B build/Release \
        -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
        -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build/Release --target all -- -j${_jobs}

# Tracy profiler port (active only when built with --build-arg WITH_TRACY=true)
EXPOSE 8086

WORKDIR /app/build/Release
CMD [ "./server" ]
