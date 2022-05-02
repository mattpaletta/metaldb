#pragma once

#include "expr.hpp"
#include <string>

namespace metaldb::QueryEngine::AST {
    class Read final : public Expr {
    public:
        Read(const std::string& tableName) : _tableName(tableName) {}
        ~Read() = default;

    private:
        std::string _tableName;
    };
}
