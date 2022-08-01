#pragma once

#include "column_type.h"

#include <string>
#include <limits>

namespace metaldb::QueryEngine {
    class ColumnDefinition {
    public:
        ColumnDefinition(std::string name_, metaldb::ColumnType type_) : name(std::move(name_)), type(type_) {}
        ColumnDefinition(std::string name_, std::size_t size_, metaldb::ColumnType type_) : name(std::move(name_)), maxSize(size_), type(type_) {}
        ColumnDefinition(std::string name_, metaldb::ColumnType type_, bool nullable_) : name(std::move(name_)), type(type_), nullable(nullable_) {}

        std::string name;
        std::size_t maxSize = std::numeric_limits<std::size_t>::max();
        metaldb::ColumnType type;
        bool nullable = false;
    };
}
