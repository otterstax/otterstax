// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "frontend/mysql_server/packet/packet_reader.hpp"
#include "frontend/mysql_server/packet/packet_utils.hpp"
#include "frontend/mysql_server/packet/packet_writer.hpp"

#include <catch2/catch_all.hpp>

using namespace frontend::mysql;

TEST_CASE("packet_writer: basic operations") {
    packet_writer writer;

    SECTION("write & read uint8") {
        writer.write_uint8(42);
        writer.write_uint8(255);

        auto packet = writer.build_from_payload(1);

        // Check header: length (2 bytes) + sequence_id
        REQUIRE(packet.size() == 6); // 4 header + 2 data
        REQUIRE(packet[0] == 2);     // length = 2
        REQUIRE(packet[1] == 0);
        REQUIRE(packet[2] == 0);
        REQUIRE(packet[3] == 1); // sequence_id
        REQUIRE(packet[4] == 42);
        REQUIRE(packet[5] == 255);
    }

    SECTION("write uint16_le") {
        writer.write_uint16(0x1234);

        auto packet = writer.build_from_payload(2);

        REQUIRE(packet[4] == 0x34); // little endian
        REQUIRE(packet[5] == 0x12);
    }

    SECTION("write uint32_le") {
        writer.write_uint32(0x12345678);

        auto packet = writer.build_from_payload(3);

        REQUIRE(packet[4] == 0x78);
        REQUIRE(packet[5] == 0x56);
        REQUIRE(packet[6] == 0x34);
        REQUIRE(packet[7] == 0x12);
    }
}

TEST_CASE("packet_writer: reserve_payload") {
    packet_writer writer;

    SECTION("reserved payload efficiency") {
        writer.reserve_payload(10);
        writer.write_uint8(1);
        writer.write_uint8(2);

        auto packet = writer.build_from_payload(5);
        REQUIRE(packet.size() == 6); // 4 header + 2 data
        REQUIRE(packet[3] == 5);     // sequence_id
        REQUIRE(packet[4] == 1);
        REQUIRE(packet[5] == 2);
    }

    SECTION("without reserve still works") {
        writer.write_uint8(100);
        writer.write_uint8(200);

        auto packet = writer.build_from_payload(10);
        REQUIRE(packet.size() == 6);
        REQUIRE(packet[3] == 10); // sequence_id
        REQUIRE(packet[4] == 100);
        REQUIRE(packet[5] == 200);
    }
}

TEST_CASE("packet_writer: string") {
    packet_writer writer;

    SECTION("write_string_null") {
        writer.write_string_null("hello");

        auto packet = writer.build_from_payload(1);

        // "hello" + null terminator = 6 bytes
        REQUIRE(packet.size() == 10); // 4 header + 6 data
        REQUIRE(packet[4] == 'h');
        REQUIRE(packet[5] == 'e');
        REQUIRE(packet[6] == 'l');
        REQUIRE(packet[7] == 'l');
        REQUIRE(packet[8] == 'o');
        REQUIRE(packet[9] == 0); // null terminator
    }

    SECTION("write_string_fixed") {
        writer.write_string_fixed("test");

        auto packet = writer.build_from_payload(1);

        REQUIRE(packet.size() == 8); // 4 header + 4 data
        REQUIRE(packet[4] == 't');
        REQUIRE(packet[5] == 'e');
        REQUIRE(packet[6] == 's');
        REQUIRE(packet[7] == 't');
    }

    SECTION("write_length_encoded_string") {
        writer.write_length_encoded_string("abc");

        auto packet = writer.build_from_payload(1);

        // 1 byte length + 3 bytes string = 4 bytes
        REQUIRE(packet.size() == 8);
        REQUIRE(packet[4] == 3); // length
        REQUIRE(packet[5] == 'a');
        REQUIRE(packet[6] == 'b');
        REQUIRE(packet[7] == 'c');
    }
}

TEST_CASE("packet_writer: length encoded integers") {
    packet_writer writer;

    SECTION("1-byte length encoded integer") {
        writer.write_length_encoded_integer(250);

        auto packet = writer.build_from_payload(1);

        REQUIRE(packet.size() == 5);
        REQUIRE(packet[4] == 250);
    }

    SECTION("3-byte length encoded integer") {
        writer.write_length_encoded_integer(300);

        auto packet = writer.build_from_payload(1);

        REQUIRE(packet.size() == 7); // header + marker + 2 bytes
        REQUIRE(packet[4] == 0xFC);  // 2-byte marker
        REQUIRE(packet[5] == 44);    // 300 & 0xFF = 44
        REQUIRE(packet[6] == 1);     // 300 >> 8 = 1
    }

    SECTION("9-byte length encoded integer") {
        writer.write_length_encoded_integer(0x123456789ABCDEF0ULL);

        auto packet = writer.build_from_payload(1);

        REQUIRE(packet.size() == 13); // 4 header + 1 marker + 8 bytes
        REQUIRE(packet[4] == 0xFE);   // 8-byte marker
        // Little endian 8 bytes:
        REQUIRE(packet[5] == 0xF0);  // byte 0
        REQUIRE(packet[6] == 0xDE);  // byte 1
        REQUIRE(packet[7] == 0xBC);  // byte 2
        REQUIRE(packet[8] == 0x9A);  // byte 3
        REQUIRE(packet[9] == 0x78);  // byte 4
        REQUIRE(packet[10] == 0x56); // byte 5
        REQUIRE(packet[11] == 0x34); // byte 6
        REQUIRE(packet[12] == 0x12); // byte 7
    }
}

TEST_CASE("packet_reader: basic operations") {
    SECTION("read uint8") {
        std::vector<uint8_t> data = {42, 255, 0};
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_uint8() == 42);
        REQUIRE(reader.read_uint8() == 255);
        REQUIRE(reader.read_uint8() == 0);
        REQUIRE(reader.remaining() == 0);
    }

    SECTION("read uint16_le") {
        std::vector<uint8_t> data = {0x34, 0x12}; // little endian 0x1234
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_uint16() == 0x1234);
    }

    SECTION("read uint32_le") {
        std::vector<uint8_t> data = {0x78, 0x56, 0x34, 0x12}; // little endian 0x12345678
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_uint32() == 0x12345678);
    }
}

TEST_CASE("packet_reader: string operations") {
    SECTION("read_string_null") {
        std::vector<uint8_t> data = {'h', 'e', 'l', 'l', 'o', 0, 'w', 'o', 'r', 'l', 'd', 0};
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_string_null() == "hello");
        REQUIRE(reader.read_string_null() == "world");
        REQUIRE(reader.remaining() == 0);
    }

    SECTION("read_string_eof") {
        std::vector<uint8_t> data = {'r', 'e', 's', 't'};
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_string_eof() == "rest");
        REQUIRE(reader.remaining() == 0);
    }

    SECTION("read_length_encoded_string") {
        std::vector<uint8_t> data = {3, 'a', 'b', 'c'};
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_length_encoded_string() == "abc");
        REQUIRE(reader.remaining() == 0);
    }

    SECTION("read_length_encoded_string empty") {
        std::vector<uint8_t> data = {0};
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_length_encoded_string() == "");
        REQUIRE(reader.remaining() == 0);
    }
}

TEST_CASE("packet_reader: length encoded integers") {
    SECTION("1-byte length encoded integer") {
        std::vector<uint8_t> data = {250};
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_length_encoded_integer() == 250);
    }

    SECTION("3-byte length encoded integer") {
        std::vector<uint8_t> data = {0xFC, 44, 1}; // 300 in little endian
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_length_encoded_integer() == 300);
    }

    SECTION("4-byte length encoded integer") {
        std::vector<uint8_t> data = {0xFD, 0x00, 0x01, 0x00}; // 256 in 3-byte little endian
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_length_encoded_integer() == 256);
    }

    SECTION("9-byte length encoded integer") {
        std::vector<uint8_t> data = {0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00};
        packet_reader reader(std::move(data));

        REQUIRE(reader.read_length_encoded_integer() == 0xFFFFFFFF);
    }

    SECTION("NULL marker is an invalid_marker fault, not a value") {
        std::vector<uint8_t> data = {0xFB};
        packet_reader reader(std::move(data));

        reader.read_length_encoded_integer();
        REQUIRE_FALSE(reader.ok());
        REQUIRE(reader.fault() == frontend::packet_fault::invalid_marker);
        REQUIRE(reader.remaining() == 1);
    }

    SECTION("0xFF marker is an invalid_marker fault") {
        std::vector<uint8_t> data = {0xFF, 1, 2};
        packet_reader reader(std::move(data));

        reader.read_length_encoded_integer();
        REQUIRE(reader.fault() == frontend::packet_fault::invalid_marker);
        REQUIRE(reader.remaining() == 3);
    }
}

TEST_CASE("packet_reader: utility") {
    SECTION("skip_bytes") {
        std::vector<uint8_t> data = {1, 2, 3, 4, 5};
        packet_reader reader(std::move(data));

        reader.skip_bytes(2);
        REQUIRE(reader.read_uint8() == 3);
        REQUIRE(reader.remaining() == 2);
    }

    SECTION("remaining count") {
        std::vector<uint8_t> data = {1, 2, 3, 4, 5};
        packet_reader reader(std::move(data));

        REQUIRE(reader.remaining() == 5);
        reader.read_uint8();
        REQUIRE(reader.remaining() == 4);
        reader.skip_bytes(2);
        REQUIRE(reader.remaining() == 2);
    }
}

// A read past the end of the packet is reported through the reader's fault
// state, never thrown: the connection turns it into a protocol error.
TEST_CASE("packet_reader: error handling") {
    SECTION("read beyond bounds is an underflow fault") {
        std::vector<uint8_t> data = {1, 2};
        packet_reader reader(std::move(data));

        reader.read_uint8(); // OK
        reader.read_uint8(); // OK
        REQUIRE(reader.ok());
        reader.read_uint8();
        REQUIRE_FALSE(reader.ok());
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
    }

    SECTION("read_uint16_le beyond bounds") {
        std::vector<uint8_t> data = {1};
        packet_reader reader(std::move(data));

        reader.read_uint16();
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
        REQUIRE(reader.remaining() == 1);
    }

    SECTION("skip_bytes beyond bounds") {
        std::vector<uint8_t> data = {1, 2, 3};
        packet_reader reader(std::move(data));

        reader.skip_bytes(5);
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
        REQUIRE(reader.remaining() == 3);
    }
}

TEST_CASE("packet_reader: every primitive reports an underflow on a truncated buffer") {
    // Each primitive on a buffer one byte short of what it needs: the value is
    // not data, the fault is set, and the position has not moved.
    SECTION("read_uint8 on an empty buffer") {
        packet_reader reader(std::vector<uint8_t>{});
        reader.read_uint8();
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
        REQUIRE(reader.remaining() == 0);
    }

    SECTION("read_int16 / read_uint16") {
        packet_reader a(std::vector<uint8_t>{0x01});
        a.read_int16();
        REQUIRE(a.fault() == frontend::packet_fault::underflow);
        packet_reader b(std::vector<uint8_t>{0x01});
        b.read_uint16();
        REQUIRE(b.fault() == frontend::packet_fault::underflow);
    }

    SECTION("read_int32 / read_uint32") {
        packet_reader a(std::vector<uint8_t>{1, 2, 3});
        a.read_int32();
        REQUIRE(a.fault() == frontend::packet_fault::underflow);
        REQUIRE(a.remaining() == 3);
        packet_reader b(std::vector<uint8_t>{1, 2, 3});
        b.read_uint32();
        REQUIRE(b.fault() == frontend::packet_fault::underflow);
    }

    SECTION("read_int64 / read_uint64") {
        packet_reader a(std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7});
        a.read_int64();
        REQUIRE(a.fault() == frontend::packet_fault::underflow);
        REQUIRE(a.remaining() == 7);
        packet_reader b(std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7});
        b.read_uint64();
        REQUIRE(b.fault() == frontend::packet_fault::underflow);
    }

    SECTION("read_string_null without a terminator") {
        packet_reader reader(std::vector<uint8_t>{'a', 'b', 'c'});
        REQUIRE(reader.read_string_null().empty());
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
        REQUIRE(reader.remaining() == 3);
    }

    SECTION("read_string_null on an empty buffer") {
        packet_reader reader(std::vector<uint8_t>{});
        reader.read_string_null();
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
    }

    SECTION("read_string_eof never faults") {
        packet_reader reader(std::vector<uint8_t>{});
        REQUIRE(reader.read_string_eof().empty());
        REQUIRE(reader.ok());
    }

    SECTION("length-encoded integer markers without their payload") {
        packet_reader two(std::vector<uint8_t>{0xFC, 0x01});
        two.read_length_encoded_integer();
        REQUIRE(two.fault() == frontend::packet_fault::underflow);
        REQUIRE(two.remaining() == 2);

        packet_reader three(std::vector<uint8_t>{0xFD, 0x01, 0x02});
        three.read_length_encoded_integer();
        REQUIRE(three.fault() == frontend::packet_fault::underflow);
        REQUIRE(three.remaining() == 3);

        packet_reader eight(std::vector<uint8_t>{0xFE, 1, 2, 3, 4, 5, 6, 7});
        eight.read_length_encoded_integer();
        REQUIRE(eight.fault() == frontend::packet_fault::underflow);
        REQUIRE(eight.remaining() == 8);

        packet_reader empty(std::vector<uint8_t>{});
        empty.read_length_encoded_integer();
        REQUIRE(empty.fault() == frontend::packet_fault::underflow);
    }

    SECTION("length-encoded string longer than the buffer") {
        packet_reader reader(std::vector<uint8_t>{5, 'a', 'b'});
        REQUIRE(reader.read_length_encoded_string().empty());
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
        REQUIRE(reader.remaining() == 3);
    }

    SECTION("length-encoded string whose 8-byte length would overflow the bounds arithmetic") {
        packet_reader reader(std::vector<uint8_t>{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 'x'});
        REQUIRE(reader.read_length_encoded_string().empty());
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
    }

    SECTION("the fault is sticky: a later in-bounds read fails too") {
        packet_reader reader(std::vector<uint8_t>{1, 2, 3});
        reader.read_uint32();
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
        reader.read_uint8();
        REQUIRE(reader.fault() == frontend::packet_fault::underflow);
        REQUIRE(reader.remaining() == 3);
        reader.skip_bytes(1);
        REQUIRE(reader.remaining() == 3);
    }

    SECTION("an exact fit is not a fault") {
        packet_reader reader(std::vector<uint8_t>{1, 2, 3, 4, 5, 6, 7, 8, 'a', 0});
        REQUIRE(reader.read_uint64() == 0x0807060504030201ULL);
        REQUIRE(reader.read_string_null() == "a");
        REQUIRE(reader.ok());
        REQUIRE(reader.remaining() == 0);
    }
}

TEST_CASE("round-trip writer -> reader") {
    packet_writer writer;
    writer.write_uint8(42);
    writer.write_uint16(0x1234);
    writer.write_string_null("hello");
    writer.write_length_encoded_integer(300);
    writer.write_length_encoded_string("world");

    auto packet = writer.build_from_payload(5);

    // Skip header and read back
    std::vector<uint8_t> payload(packet.begin() + 4, packet.end());
    packet_reader reader(std::move(payload));

    REQUIRE(reader.read_uint8() == 42);
    REQUIRE(reader.read_uint16() == 0x1234);
    REQUIRE(reader.read_string_null() == "hello");
    REQUIRE(reader.read_length_encoded_integer() == 300);
    REQUIRE(reader.read_length_encoded_string() == "world");
    REQUIRE(reader.remaining() == 0);
}

// An OK packet carries affected_rows and last_insert_id as length-encoded
// integers; a single byte holds a count only up to 250 (0xFB..0xFF are markers).
TEST_CASE("build_ok: affected rows are a length-encoded integer") {
    for (uint64_t affected : {uint64_t{0}, uint64_t{250}, uint64_t{251}, uint64_t{1000}, uint64_t{2000}, uint64_t{70000}}) {
        packet_writer writer;
        auto packet = build_ok(writer, 1, affected);

        std::vector<uint8_t> payload(packet.begin() + 4, packet.end());
        packet_reader reader(std::move(payload));

        REQUIRE(reader.read_uint8() == 0x00);                      // OK header
        REQUIRE(reader.read_length_encoded_integer() == affected); // affected_rows
        REQUIRE(reader.read_length_encoded_integer() == 0);        // last_insert_id
        REQUIRE(reader.read_uint16() == 2);                        // SERVER_STATUS_AUTOCOMMIT
        REQUIRE(reader.read_uint16() == 0);                        // warnings
        REQUIRE(reader.remaining() == 0);
        REQUIRE(reader.ok());
    }
}
