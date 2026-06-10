// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#include "execute_plan.hpp"

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "utility/tracy_profiler.hpp"
#include <components/logical_plan/node_data.hpp>

using namespace components;
OtterbrixDataManager::OtterbrixDataManager(otterbrix::otterbrix_ptr otterbrix)
    : otterbrix_(otterbrix) {}

components::cursor::cursor_t_ptr OtterbrixDataManager::execute_plan(OtterbrixStatementPtr& otterbrix_params) {
    OTX_ZONE_N("otterbrix::execute_plan");

    return otterbrix_->dispatcher()->execute_plan(otterbrix::session_id_t(),
                                                  otterbrix_params->node,
                                                  otterbrix_params->params_node);
}

components::cursor::cursor_t_ptr OtterbrixDataManager::get_schema(const OtterbrixSchemaParams& otterbrix_params) {
    OTX_ZONE_N("otterbrix::get_schema_impl");

    return otterbrix_->dispatcher()->get_schema(otterbrix::session_id_t(), otterbrix_params);
}