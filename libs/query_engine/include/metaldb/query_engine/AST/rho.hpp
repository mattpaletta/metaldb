#pragma once

#include "expr.hpp"

#include <string>
#include <memory>

namespace metaldb::QueryEngine::AST {
    class Rho : public Expr {
    public:
        Rho(std::string originalName, std::string renamedName, std::shared_ptr<Expr> parent) : _originalName(std::move(originalName)), _renamedName(std::move(renamedName)), _parent(std::move(parent)) {}
        ~Rho() noexcept = default;

    private:
        std::string _originalName;
        std::string _renamedName;
        std::shared_ptr<Expr> _parent;
    };
}
