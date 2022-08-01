#pragma once

#include "expr.hpp"

#include <memory>

namespace metaldb::QueryEngine::AST {
    class Limit : public Expr {
    public:
        Limit(std::size_t value, std::shared_ptr<Expr> parent) : _value(value), _parent(std::move(parent)) {}
        ~Limit() noexcept = default;

    private:
        std::size_t _value;
        std::shared_ptr<Expr> _parent;
    };
}
