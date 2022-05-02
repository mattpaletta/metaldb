#pragma once

#include "filter_expr.hpp"
#include "expr.hpp"

#include <memory>

namespace metaldb::QueryEngine::AST {
    class Join : public Expr {
    public:
        enum JoinType {
            LEFT,
            RIGHT,
            NATURAL
        };

        Join(JoinType joinType, std::shared_ptr<BaseFilterExpr> expr, std::shared_ptr<Expr> lhs, std::shared_ptr<Expr> rhs) : _type(joinType), _expr(expr), _lhs(lhs), _rhs(rhs) {}
        ~Join() = default;

    private:
        JoinType _type;
        std::shared_ptr<BaseFilterExpr> _expr;
        std::shared_ptr<Expr> _lhs;
        std::shared_ptr<Expr> _rhs;
    };
}
