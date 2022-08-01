#pragma once

#include "table_definition.hpp"

#include <vector>

namespace metaldb::QueryEngine {
    struct Metadata {
        const TableDefinition* getTable(const std::string& name) const noexcept {
            for (const auto& table : this->tables) {
                if (table.name == name) {
                    return &table;
                }
            }
            return nullptr;
        }

        std::vector<TableDefinition> tables;
    };
}
