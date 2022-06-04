//
//  table_definition.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-05-07.
//

#pragma once

#include "column_definition.hpp"

#include <string>
#include <vector>

namespace metaldb::QueryEngine {
    struct TableDefinition {
        const ColumnDefinition* getColumnDefinition(const std::string& column) const {
            for (auto& c : this->columns) {
                if (c.name == column) {
                    return &c;
                }
            }

            return nullptr;
        }


        std::string name;
        std::string filePath;

        std::vector<ColumnDefinition> columns;
    };
}
