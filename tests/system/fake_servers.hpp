// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax
//
// Fake backend servers behind the session stack: the connector factories of
// remote_backends_t (connector_factories_t) build the fake connectors of
// tests/mock/fake_backend.hpp over the fake server of the connection's alias,
// so a test configures a MySQL / PostgreSQL connection in `connections:` as
// usual, the startup discovers the server's canned tables through the agents
// (sql_mode, the information_schema listing, the `WHERE 1 = 0` probes), and
// every statement an agent sends is in the server's log (`sent(alias)`: the
// reads and writes as the backend received them).
//
// The servers live in a `fake_servers_t` the test declares BEFORE its
// session_stack_owner (the stack's connectors and the chunks they answered
// refer to them until the stack is gone). A canned read answers every row of
// the table whose name the statement mentions and the columns its SELECT list
// names; a statement whose rows depend on more than that (a pushed WHERE, an
// aggregate) is answered with `answer(alias, sql, columns, rows)`: what the
// backend would answer for exactly that text.

#pragma once

#include "backends/remote_backends.hpp"
#include "session_stack.hpp"

#include "../mock/fake_backend.hpp"

#include <catch2/catch_all.hpp>

#include <atomic>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace otterstax::test {

    class fake_servers_t {
    public:
        fake_servers_t() { registry() = this; }
        ~fake_servers_t() { registry() = nullptr; }
        fake_servers_t(const fake_servers_t&) = delete;
        fake_servers_t& operator=(const fake_servers_t&) = delete;

        // The server of the connection `alias` (created on first use).
        fakebackend::fake_backend_t& add(const std::string& alias, backend_type_t kind) {
            std::lock_guard guard(mutex_);
            auto& slot = servers_[alias];
            if (!slot.server) {
                slot.server = std::make_unique<fakebackend::fake_backend_t>(kind);
            }
            return *slot.server;
        }
        fakebackend::fake_backend_t& server(const std::string& alias) {
            std::lock_guard guard(mutex_);
            auto found = servers_.find(alias);
            REQUIRE(found != servers_.end());
            return *found->second.server;
        }

        // The statements the server of `alias` received, as it received them (the reads, the
        // writes and the DDL, without the log's "scan " / "write " / "exec " mark and its " @txn"
        // one; no transaction step), in order.
        std::vector<std::string> sent(const std::string& alias) {
            std::vector<std::string> out;
            for (auto line : server(alias).log()) {
                for (const char* mark : {"scan ", "write ", "exec "}) {
                    if (line.rfind(mark, 0) == 0) {
                        line = line.substr(std::string{mark}.size());
                        if (line.size() > 5 && line.compare(line.size() - 5, 5, " @txn") == 0) {
                            line.resize(line.size() - 5);
                        }
                        out.push_back(line);
                    }
                }
            }
            return out;
        }
        void forget_sent() {
            std::lock_guard guard(mutex_);
            for (auto& [alias, slot] : servers_) {
                slot.server->clear();
            }
        }

        // `sql` (exactly) is answered with `rows` of `columns` by the server of `alias`.
        void answer(const std::string& alias,
                    std::string sql,
                    std::vector<components::types::complex_logical_type> columns,
                    std::vector<fakebackend::row_t> rows) {
            std::lock_guard guard(mutex_);
            auto& slot = servers_.at(alias);
            slot.answers->emplace(std::move(sql), answer_t{std::move(columns), std::move(rows)});
            auto* answers = slot.answers.get();
            slot.server->on_scan([answers](const std::string& query, std::pmr::memory_resource* resource)
                                     -> std::optional<components::vector::data_chunk_t> {
                auto found = answers->find(query);
                if (found == answers->end()) {
                    return std::nullopt;
                }
                return fakebackend::chunk_of(resource, found->second.columns, found->second.rows);
            });
        }

        // The connector factories of the fake servers (MySQL and PostgreSQL; ClickHouse keeps the
        // mock connector).
        static backends::connector_factories_t connectors() {
            backends::connector_factories_t out = mock_connectors();
            out.mysql = &mysql_factory;
            out.postgres = &pg_factory;
            return out;
        }

    private:
        struct answer_t {
            std::vector<components::types::complex_logical_type> columns;
            std::vector<fakebackend::row_t> rows;
        };
        struct slot_t {
            std::unique_ptr<fakebackend::fake_backend_t> server;
            // Read on the agents' threads under the server's own lock (on_scan), written by the
            // test thread before the statement it answers is sent.
            std::unique_ptr<std::map<std::string, answer_t>> answers = std::make_unique<std::map<std::string, answer_t>>();
        };

        static fake_servers_t*& registry() {
            static fake_servers_t* current = nullptr;
            return current;
        }
        static int next_number() {
            static std::atomic<int> number{0};
            return ++number;
        }
        static fakebackend::fake_backend_t& server_of(const std::string& alias, backend_type_t kind) {
            auto* current = registry();
            if (current == nullptr) {
                std::abort(); // a fake connector built with no fake_servers_t alive
            }
            return current->add(alias, kind);
        }
        static std::unique_ptr<mysql::IConnector>
        mysql_factory(std::pmr::memory_resource*, boost::asio::io_context&, boost::mysql::connect_params, std::string alias) {
            return std::make_unique<fakebackend::fake_mysql_t>(server_of(alias, backend_type_t::MySQL), next_number());
        }
        static std::unique_ptr<pg::IConnector> pg_factory(std::pmr::memory_resource*, pg::connect_params, std::string alias) {
            return std::make_unique<fakebackend::fake_pg_t>(server_of(alias, backend_type_t::PostgreSQL), next_number());
        }

        std::mutex mutex_;
        std::map<std::string, slot_t> servers_;
    };

} // namespace otterstax::test
