// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

#include <spark/connect/base.pb.h>
#include <spark/connect/relations.pb.h>

#include "otterbrix/parser/parser.hpp"
#include "types/otterbrix.hpp"

namespace frontend::spark {

struct TranslationResult {
    ParsedQueryDataPtr parsed_data;
};

core::result_wrapper_t<TranslationResult>
relation_to_plan(const ::spark::connect::Plan& plan,
                 std::pmr::memory_resource* resource);

bool contains_window(const ::spark::connect::Relation& rel);

}  // namespace frontend::spark
