#pragma once

#include "expr.hpp"
#include "filter_expr.hpp"

#include <memory>

namespace metaldb::QueryEngine::AST {
    class Filter : public Expr {
    public:
        Filter(std::shared_ptr<BaseFilterExpr> expr, std::shared_ptr<Expr> parent) : _expr(expr), _parent(parent) {}
        ~Filter() = default;

    private:
        std::shared_ptr<BaseFilterExpr> _expr;
        std::shared_ptr<Expr> _parent;
    };
}
