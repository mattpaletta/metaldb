#pragma once

#include "expr.hpp"
#include <string>

namespace metaldb::QueryEngine::AST {
    class Read final : public Expr {
    public:
        Read(std::string tableName) : _tableName(std::move(tableName)) {}
        ~Read() noexcept = default;

        std::string tableName() const noexcept {
            return this->_tableName;
        }

    private:
        std::string _tableName;
    };
}
