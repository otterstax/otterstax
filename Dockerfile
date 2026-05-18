FROM ubuntu:22.04 AS builder

ENV TZ=America/US
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONFAULTHANDLER=1

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
RUN conan install conanfile.py --build missing \
    -s build_type=Release \
    -s compiler.cppstd=20 \
    -s 'clickhouse-cpp/*:compiler.cppstd=17' \
    -s 'abseil/*:compiler.cppstd=17' \
    -s 'grpc/*:compiler.cppstd=17'

COPY ./catalog ./catalog
COPY ./config ./config
COPY ./connectors ./connectors
COPY ./db_integration ./db_integration
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

WORKDIR /app/build/Release
CMD [ "./server" ]
