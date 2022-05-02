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
        ColumnInfo(const std::string& name_, ColumnType type_) : name(name_), type(type_) {}

        std::string name;
        ColumnType type;
    };

    class QueryInfo final {
    public:
        QueryInfo(const std::vector<ColumnInfo>& columns_, const std::string& tableLocation) : columns(columns_) {}
        ~QueryInfo() = default;

        std::vector<ColumnInfo> columns;
    };
}
