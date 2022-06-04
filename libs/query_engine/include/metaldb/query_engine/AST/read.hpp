#pragma once

#include "expr.hpp"
#include <string>

namespace metaldb::QueryEngine::AST {
    class Read final : public Expr {
    public:
        Read(const std::string& tableName) : _tableName(tableName) {}
        ~Read() = default;

        std::string tableName() const {
            return this->_tableName;
        }

    private:
        std::string _tableName;
    };
}
