FROM ubuntu:22.04 AS builder

ENV TZ=America/US
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONFAULTHANDLER=1

ARG WITH_TRACY=false

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

RUN pip3 install --no-cache-dir conan==2.15.0 'cmake<4.0' && \
     conan profile detect --force && \
     conan remote add otterbrix http://conan.otterbrix.com

RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# Run conan install from the project root so self.source_folder=/app and
# cmake_layout places generators + libs at build/Release/ — matching the
# devcontainer preset layout exactly.
WORKDIR /app
COPY conanfile.py .
RUN if [ "$WITH_TRACY" = "true" ]; then \
        conan install conanfile.py --build missing \
            -s build_type=Release \
            -s compiler.cppstd=20 \
            -s 'clickhouse-cpp/*:compiler.cppstd=17' \
            -s 'abseil/*:compiler.cppstd=17' \
            -s 'grpc/*:compiler.cppstd=17' \
            -o "&:with_tracy=True"; \
    else \
        conan install conanfile.py --build missing \
            -s build_type=Release \
            -s compiler.cppstd=20 \
            -s 'clickhouse-cpp/*:compiler.cppstd=17' \
            -s 'abseil/*:compiler.cppstd=17' \
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
COPY ./component_manager ./component_manager
COPY ./scheduler ./scheduler
COPY ./types ./types
COPY ./utility ./utility
COPY ./main.cpp ./main.cpp
COPY ./CMakeLists.txt ./CMakeLists.txt

RUN cmake -S . -B build/Release \
        -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
        -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build/Release --target all -- -j 5

# Tracy profiler port (active only when built with --build-arg WITH_TRACY=true)
EXPOSE 8086

WORKDIR /app/build/Release
CMD [ "./server" ]
