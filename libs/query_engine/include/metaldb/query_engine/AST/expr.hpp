#pragma once

namespace metaldb::QueryEngine::AST {
    class Expr {
    public:
        Expr() = default;
        virtual ~Expr() = default;
    };
}
