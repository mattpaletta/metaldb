#pragma once

#include "expr.hpp"

#include <string>
#include <vector>
#include <memory>

namespace metaldb::QueryEngine::AST {
    class Projection final : public Expr {
    public:
        Projection(const std::vector<std::string>& columns, std::shared_ptr<Expr> parent) : _columns(columns), _parent(parent) {}
        ~Projection() = default;

    private:
        std::vector<std::string> _columns;
        std::shared_ptr<Expr> _parent;
    };
}
