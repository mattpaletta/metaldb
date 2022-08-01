#pragma once

#include <vector>
#include <string>

namespace metaldb::QueryEngine {
    enum ColumnType {
        STRING,
        FLOAT,
        INTEGER
    };

    class ColumnInfo {
    public:
        ColumnInfo(std::string name_, ColumnType type_) : name(std::move(name_)), type(type_) {}

        std::string name;
        ColumnType type;
    };

    class QueryInfo final {
    public:
        QueryInfo(std::vector<ColumnInfo> columns_, std::string tableLocation) : columns(std::move(columns_)) {}
        ~QueryInfo() = default;

        std::vector<ColumnInfo> columns;
    };
}
