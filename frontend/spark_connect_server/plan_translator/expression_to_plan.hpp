// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

// Translates a Spark Connect `spark::connect::Expression` protobuf into an
// Otterbrix logical-plan expression tree (`components::logical_plan::expression_ptr`).
//
// Used by relation_to_plan (Path B) to build the Otterbrix logical plan directly
// from the Spark Connect plan without round-tripping through the SQL text parser.
//
// Literal values are materialised into the supplied `parameter_node_t` and
// referenced from the resulting expression. Unsupported expression variants
// (Window, PythonUDF/ScalaUDF, lambdas, subqueries, ...) yield a `core::error_t`
// instead of throwing.

#include <components/logical_plan/node.hpp>          // components::logical_plan::expression_ptr
#include <components/logical_plan/param_storage.hpp> // components::logical_plan::parameter_node_ptr
#include <core/result_wrapper.hpp>                   // core::result_wrapper_t

#include <memory_resource>

namespace spark::connect {
class Expression;
}  // namespace spark::connect

namespace frontend::spark {

// Converts a Spark Expression proto to an Otterbrix expression tree.
// Returns an error for unsupported expression types (Window, UDF, etc.).
core::result_wrapper_t<components::logical_plan::expression_ptr>
expression_to_plan(const ::spark::connect::Expression& expr,
                   components::logical_plan::parameter_node_ptr params,
                   std::pmr::memory_resource* resource);

}  // namespace frontend::spark
