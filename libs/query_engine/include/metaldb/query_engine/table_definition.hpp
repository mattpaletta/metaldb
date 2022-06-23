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
#include <optional>

namespace metaldb::QueryEngine {
    struct TableDefinition {
        std::optional<std::size_t> getColumnIndex(const std::string& column) const {
            for (std::size_t i = 0; i < this->columns.size(); ++i) {
                if (this->columns.at(i).name == column) {
                    return i;
                }
            }
            return std::nullopt;
        }

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
