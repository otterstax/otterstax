# SPDX-License-Identifier: Apache-2.0
# Copyright 2025-2026 OtterStax

"""Unsupported Spark operations must fail explicitly through every client version."""

import argparse
import threading
import uuid
import zlib

import grpc
import config
from pyspark.sql.connect.proto import base_pb2, base_pb2_grpc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args()
    address = f"{config.get_host(args.local)}:{config.SPARK_CONNECT_PORT}"
    failures = []

    def unsupported(name, operation):
        try:
            operation()
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.UNIMPLEMENTED:
                failures.append(f"{name}: {error.code()} {error.details()}")
            else:
                print(f"PASS {name}: explicit unsupported-operation error")
        else:
            failures.append(f"{name}: unsupported operation reported success")

    session = str(uuid.uuid4())
    with grpc.insecure_channel(address) as channel:
        grpc.channel_ready_future(channel).result(timeout=10)
        stub = base_pb2_grpc.SparkConnectServiceStub(channel)
        for kind in (base_pb2.InterruptRequest.INTERRUPT_TYPE_ALL,
                     base_pb2.InterruptRequest.INTERRUPT_TYPE_TAG,
                     base_pb2.InterruptRequest.INTERRUPT_TYPE_OPERATION_ID):
            request = base_pb2.InterruptRequest(session_id=session, interrupt_type=kind)
            if kind == base_pb2.InterruptRequest.INTERRUPT_TYPE_TAG:
                request.operation_tag = "capability-test"
            elif kind == base_pb2.InterruptRequest.INTERRUPT_TYPE_OPERATION_ID:
                request.operation_id = str(uuid.uuid4())
            unsupported(f"Interrupt/{kind}", lambda: stub.Interrupt(request, timeout=5))

        request = base_pb2.ArtifactStatusesRequest(session_id=session, names=["files/fixture.txt"])
        unsupported("ArtifactStatus", lambda: stub.ArtifactStatus(request, timeout=5))

        artifact = base_pb2.AddArtifactsRequest(session_id=session)
        item = artifact.batch.artifacts.add(name="files/fixture.txt")
        item.data.data = b"artifact fixture"
        item.data.crc = zlib.crc32(item.data.data)
        unsupported("AddArtifacts/complete", lambda: stub.AddArtifacts(iter([artifact]), timeout=5))

        # The server must refuse unsupported uploads without draining an arbitrary
        # client stream. Keep the stream open until the RPC has returned an error.
        release = threading.Event()

        def incomplete_upload():
            yield artifact
            release.wait(timeout=10)

        call = stub.AddArtifacts.future(incomplete_upload(), timeout=5)
        try:
            unsupported("AddArtifacts/open stream", call.result)
        finally:
            release.set()
            call.cancel()

        request = base_pb2.ConfigRequest(session_id=session)
        request.operation.get.keys.append("spark.sql.session.timeZone")
        response = stub.Config(request, timeout=5)
        assert [(pair.key, pair.value) for pair in response.pairs] == [("spark.sql.session.timeZone", "UTC")]
    assert not failures, "\n".join(failures)
    print("PASS supported RPC remains usable after capability refusals")


if __name__ == "__main__":
    main()
