#pragma once

#include "../table_definition.hpp"

namespace metaldb::QueryEngine::AST {
    class Expr {
    public:
        Expr() = default;
        virtual ~Expr() noexcept = default;
    };
}
