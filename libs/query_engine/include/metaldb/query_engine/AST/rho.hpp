#pragma once

#include "expr.hpp"

#include <string>
#include <memory>

namespace metaldb::QueryEngine::AST {
    class Rho : public Expr {
    public:
        Rho(const std::string& originalName, const std::string& renamedName, std::shared_ptr<Expr> parent) : _originalName(originalName), _renamedName(renamedName), _parent(parent) {}
        ~Rho() = default;

    private:
        std::string _originalName;
        std::string _renamedName;
        std::shared_ptr<Expr> _parent;
    };
}
