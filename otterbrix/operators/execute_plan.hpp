// SPDX-License-Identifier: Apache-2.0
// Copyright 2025-2026  OtterStax

#pragma once

#include <components/cursor/cursor.hpp>
#include <otterbrix/otterbrix.hpp>

#include "otterbrix/translators/input/mysql_to_chunk.hpp"
#include "types/otterbrix.hpp"

class IDataManager {
public:
    virtual ~IDataManager() = default;

    virtual components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) = 0;
    virtual components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) = 0;
};

class OtterbrixDataManager : public IDataManager {
public:
    explicit OtterbrixDataManager(otterbrix::otterbrix_ptr otterbrix);

    components::cursor::cursor_t_ptr execute_plan(OtterbrixStatementPtr& otterbrix_params) override;
    components::cursor::cursor_t_ptr get_schema(const OtterbrixSchemaParams& otterbrix_params) override;

private:
    otterbrix::otterbrix_ptr otterbrix_;
};

using data_manager_ptr = std::unique_ptr<OtterbrixDataManager>;

inline data_manager_ptr make_otterbrix_manager(otterbrix::otterbrix_ptr otterbrix) {
    return std::make_unique<OtterbrixDataManager>(std::move(otterbrix));
}
