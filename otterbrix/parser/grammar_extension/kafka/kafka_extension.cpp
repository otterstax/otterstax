// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "kafka_extension.hpp"

#include "kafka_ast.hpp"
#include "kafka_gram.hpp"
#include "kafka_node.hpp"
#include "kafka_scan.h"

#include <cassert>
#include <string>
#include <string_view>

#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/flex_scanner_guard.hpp>
#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/pg_std_list.h>

namespace {
    using kafka_scanner_t = EXTENSION_FLEX_SCANNER(kafka_yy);
} // namespace

namespace kafka_ext {
    using namespace components::sql::parser;

    parse_extension_result_t parse(std::pmr::memory_resource* resource, const std::string& query) {
        // we own only CREATE/DROP {SOURCE|STREAM}. Peek the first, two tokens
        {
            kafka_scanner_t peek(query.c_str());
            if (!peek.valid()) {
                return NIL;
            }
            kafka_yyset_extra(resource, peek.handle());
            YYSTYPE token{};
            const int first = peek.next_token(&token);
            if (first != KW_CREATE && first != KW_DROP) {
                return NIL;
            }
            const int second = peek.next_token(&token);
            if (second != KW_SOURCE && second != KW_STREAM) {
                return NIL;
            }
        }

        kafka_scanner_t scanner(query.c_str());
        if (!scanner.valid()) {
            return NIL;
        }
        kafka_yyset_extra(resource, scanner.handle());

        Node* root = nullptr;
        if (kafka_yyparse(scanner.handle(), resource, &root) != 0 || root == nullptr) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"kafka: invalid statement", resource});
        }

        // Semantic guard the grammar can't express: every declared column type
        // must map to an otterbrix logical type, since transform relies on it.
        const auto* stmt = static_cast<const kafka_grammar::kafka_stmt*>(reinterpret_cast<ExtensionNode*>(root)->data);
        for (const auto* col = stmt->columns; col != nullptr; col = col->next) {
            if (!otterstax::kafka::map_column_type(col->type).has_value()) {
                const std::string message = std::string{"kafka: unknown column type '"} + std::string(col->type) + "'";
                return core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{message.c_str(), resource});
            }
        }

        return list_make1(resource, root);
    }

    components::logical_plan::node_ptr transform(std::pmr::memory_resource* resource,
                                                 ExtensionNode* node,
                                                 components::logical_plan::parameter_node_t* /*params*/) {
        assert(node->extension_id != nullptr && std::string_view(node->extension_id) == "kafka");
        const auto* stmt = static_cast<const kafka_grammar::kafka_stmt*>(node->data);
        return otterstax::kafka::lower_to_node(resource, *stmt);
    }
} // namespace kafka_ext
