//
//  metadata.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-05-07.
//

#pragma once

#include "table_definition.hpp"

#include <vector>

namespace metaldb::QueryEngine {
    struct Metadata {

        const TableDefinition* getTable(const std::string& name) const {
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
