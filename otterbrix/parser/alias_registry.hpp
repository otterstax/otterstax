// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/base/collection_full_name.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <map>
#include <memory_resource>
#include <string>
#include <string_view>

enum class backend_type_t : uint8_t;

namespace otterstax::names {
    struct alias_t {
        backend_type_t backend;
        std::string database;
        std::string schema;
    };

    class alias_registry_t {
    public:
        void add(std::string alias, alias_t connection);
        [[nodiscard]] const alias_t* find(std::string_view alias) const;

    private:
        std::map<std::string, alias_t, std::less<>> aliases_;
    };

    // The table a reference means, from the slots the grammar filled
    // (`written`). Three segments behind a registered alias are federated
    // (`alias.db.table`), four name the alias explicitly; either way a slot the
    // backend pins must match the connection's — refused otherwise. Every other
    // reference is local and comes back as written, as does a sub-query stub; a
    // local reference naming a schema is refused, a local table has none.
    [[nodiscard]] core::result_wrapper_t<qualified_name_t> canonical_table_name(std::pmr::memory_resource* resource,
                                                                                const alias_registry_t& aliases,
                                                                                const qualified_name_t& written);

    // Table `db.rel` of the connection named `uid`, with the schema that
    // connection reaches; a PostgreSQL database other than the connection's is
    // refused. The reading canonical_table_name gives three segments, and the
    // one a transformed plan leaf (uid, db, rel) resolves to.
    [[nodiscard]] core::result_wrapper_t<qualified_name_t> federated_name(std::pmr::memory_resource* resource,
                                                                          const alias_registry_t& aliases,
                                                                          std::string_view uid,
                                                                          std::string_view db,
                                                                          std::string_view rel);
} // namespace otterstax::names
