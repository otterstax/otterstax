// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026 OtterStax
#include "sql_session.hpp"
#include "asio_future_bridge.hpp"
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>

namespace frontend {
    using namespace components::sql;

    sql_session::sql_session(std::pmr::memory_resource* resource, actor_zeta::address_t scheduler, bool mysql)
        : resource_(resource), scheduler_(scheduler), mysql_(mysql) {
        context_.persistent = true;
    }

    execution_context sql_session::context() const {
        auto value = context_;
        value.in_transaction = state_ != state::idle;
        return value;
    }

    sql_session::result sql_session::error(core::error_code_t code, const char* message) {
        return core::error_t{code, std::pmr::string{message, resource_}};
    }

    sql_session::result sql_session::await(actor_zeta::unique_future<result> future) {
        auto value = otterstax::await_future_blocking<session_payload>(std::move(future), resource_);
        if (value.has_error() && (value.error().type == otterstax::AWAIT_TIMEOUT_CODE ||
                                 value.error().type == otterstax::AWAIT_FAILED_CODE)) {
            broken_ = true;
        }
        return value;
    }

    sql_session::result sql_session::submit(std::string sql) {
        const auto request = next_request_id();
        auto [scheduled, future] = actor_zeta::send(scheduler_, &Scheduler::execute, request,
                                                    std::move(sql), context());
        return await(std::move(future));
    }

    sql_session::result sql_session::control(const char* sql, const char* command, state next) {
        result value{session_payload{resource_}};
        if (next == state::idle) {
            auto [scheduled, future] = actor_zeta::send(scheduler_, &Scheduler::finish_transaction,
                                                       context(), std::string_view{command} == "COMMIT");
            value = await(std::move(future));
        } else {
            value = submit(sql);
        }
        if (!value.has_error()) {
            state_ = next;
            if (next == state::idle) implicit_ = false;
            value.value().transaction_command = command;
        }
        return value;
    }

    sql_session::result sql_session::begin_implicit() {
        if (broken_ || closed_) return error(core::error_code_t::io_error, "connection session is unavailable");
        if (state_ != state::idle) return session_payload{resource_};
        auto value = control("BEGIN;", "BEGIN", state::active);
        if (!value.has_error()) implicit_ = true;
        return value;
    }

    sql_session::result sql_session::finish_implicit() {
        if (!implicit_) return session_payload{resource_};
        if (broken_ || closed_) return error(core::error_code_t::io_error, "connection session is unavailable");
        const bool commit = state_ == state::active;
        return control(commit ? "COMMIT;" : "ROLLBACK;", commit ? "COMMIT" : "ROLLBACK", state::idle);
    }

    std::optional<sql_session::result> sql_session::before_execute(const std::string& sql) {
        if (broken_ || closed_) return error(core::error_code_t::io_error, "connection session is unavailable");
        enum class action { ordinary, begin, commit, rollback, unsupported, autocommit_on, autocommit_off };
        action command = action::ordinary;
        try {
            std::pmr::monotonic_buffer_resource arena(resource_);
            auto list = raw_parser(&arena, sql.c_str());
            if (list_length(list) > 1) {
                return error(core::error_code_t::unimplemented_yet, "Send one SQL statement per request");
            }
            if (list_length(list) == 1) {
                auto node = linitial(list);
                if (nodeTag(node) == T_TransactionStmt) {
                    auto transaction = transform::pg_ptr_cast<TransactionStmt>(node);
                    switch (transaction->kind) {
                        case TRANS_STMT_BEGIN:
                        case TRANS_STMT_START:
                            command = transaction->options == NIL ? action::begin : action::unsupported;
                            break;
                        case TRANS_STMT_COMMIT: command = action::commit; break;
                        case TRANS_STMT_ROLLBACK: command = action::rollback; break;
                        default: command = action::unsupported; break;
                    }
                } else if (mysql_ && nodeTag(node) == T_VariableSetStmt) {
                    auto setting = transform::pg_ptr_cast<VariableSetStmt>(node);
                    if (setting->name && std::string_view{setting->name} == "autocommit") {
                        command = action::unsupported;
                        if (!setting->is_local && setting->kind == VAR_SET_VALUE && list_length(setting->args) == 1) {
                            auto arg = transform::pg_ptr_cast<A_Const>(linitial(setting->args));
                            if (nodeTag(&arg->val) == T_Integer && (intVal(&arg->val) == 0 || intVal(&arg->val) == 1)) {
                                command = intVal(&arg->val) == 0 ? action::autocommit_off : action::autocommit_on;
                            } else if (nodeTag(&arg->val) == T_String) {
                                std::string_view value{strVal(&arg->val)};
                                if (value == "on" || value == "true") command = action::autocommit_on;
                                if (value == "off" || value == "false") command = action::autocommit_off;
                            }
                        }
                    }
                }
            }
        } catch (const std::exception&) {
            // The Worker owns parsing, including the file and Kafka grammar
            // extensions. Only transaction control is interpreted here.
        }
        if (command == action::unsupported) {
            return error(core::error_code_t::unimplemented_yet,
                         "Savepoints, transaction options and two-phase transactions are not supported");
        }
        if (command == action::commit || command == action::rollback) {
            const bool rollback = command == action::rollback || state_ == state::failed;
            return control(rollback ? "ROLLBACK;" : "COMMIT;", rollback ? "ROLLBACK" : "COMMIT", state::idle);
        }
        if (state_ == state::failed) {
            return error(core::error_code_t::transaction_finalized, "Transaction is aborted; issue ROLLBACK before continuing");
        }
        if (command == action::begin) {
            if (implicit_) {
                implicit_ = false;
                session_payload value{resource_};
                value.transaction_command = "BEGIN";
                return value;
            }
            if (state_ == state::active) {
                return error(core::error_code_t::unimplemented_yet, "Nested transactions are not supported");
            }
            return control("BEGIN;", "BEGIN", state::active);
        }
        if (command == action::autocommit_off || command == action::autocommit_on) {
            const bool enabled = command == action::autocommit_on;
            if (enabled && !autocommit_ && state_ == state::active) {
                auto value = control("COMMIT;", "COMMIT", state::idle);
                if (value.has_error()) return value;
            }
            autocommit_ = enabled;
            return session_payload{resource_};
        }
        if (!autocommit_ && state_ == state::idle) {
            auto value = control("BEGIN;", "BEGIN", state::active);
            if (value.has_error()) return value;
        }
        return std::nullopt;
    }

    sql_session::result sql_session::execute(session_hash_t request, std::string sql) {
        if (auto value = before_execute(sql)) return std::move(*value);
        auto [scheduled, future] = actor_zeta::send(scheduler_, &Scheduler::execute, request, std::move(sql), context());
        return await(std::move(future));
    }

    sql_session::result sql_session::prepare(session_hash_t request, std::string sql) {
        if (broken_ || closed_) return error(core::error_code_t::io_error, "connection session is unavailable");
        if (state_ == state::failed) {
            bool ends_transaction = false;
            try {
                std::pmr::monotonic_buffer_resource arena(resource_);
                auto statements = raw_parser(&arena, sql.c_str());
                if (list_length(statements) == 1 && nodeTag(linitial(statements)) == T_TransactionStmt) {
                    auto transaction = transform::pg_ptr_cast<TransactionStmt>(linitial(statements));
                    ends_transaction = transaction->kind == TRANS_STMT_COMMIT || transaction->kind == TRANS_STMT_ROLLBACK;
                }
            } catch (const std::exception&) {
            }
            if (!ends_transaction) return error(core::error_code_t::transaction_finalized, "Transaction is aborted");
        }
        auto [scheduled, future] = actor_zeta::send(scheduler_, &Scheduler::prepare_schema, request, std::move(sql), context());
        return await(std::move(future));
    }

    sql_session::result sql_session::execute_prepared(session_hash_t request, const std::string& sql,
                   std::pmr::vector<components::types::logical_value_t> parameters) {
        if (auto value = before_execute(sql)) {
            // Transaction commands are interpreted at Execute, never at Parse.
            // Their prepared entry is still consumed just like an ordinary plan.
            auto [scheduled, future] = actor_zeta::send(scheduler_, &Scheduler::close_statement, request);
            auto closed = await(std::move(future));
            if (closed.has_error()) return closed;
            return std::move(*value);
        }
        auto [scheduled, future] = actor_zeta::send(scheduler_, &Scheduler::execute_prepared_statement,
                                                    request, std::move(parameters), context());
        return await(std::move(future));
    }

    void sql_session::mark_failed() {
        if (state_ != state::active) return;
        const bool implicit = implicit_;
        // The engine may already have aborted a failed DML. ROLLBACK is
        // idempotent and also covers parse, bind, routing and encoding errors.
        if (!broken_) {
            auto rollback = control("ROLLBACK;", "ROLLBACK", state::idle);
            if (rollback.has_error()) broken_ = true;
        }
        state_ = state::failed;
        implicit_ = implicit;
    }

    sql_session::result sql_session::close() {
        if (closed_) return session_payload{resource_};
        closed_ = true;
        auto [scheduled, future] = actor_zeta::send(scheduler_, &Scheduler::close_session, context());
        return await(std::move(future));
    }
}
