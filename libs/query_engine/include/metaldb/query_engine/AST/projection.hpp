#pragma once

#include "expr.hpp"

#include <string>
#include <vector>
#include <memory>

namespace metaldb::QueryEngine::AST {
    class Projection final : public Expr {
    public:
        Projection(std::vector<std::string> columns, std::shared_ptr<Expr> child) : _columns(std::move(columns)), _child(std::move(child)) {}
        ~Projection() noexcept = default;

        bool hasChild() const noexcept {
            return this->child().operator bool();
        }

        std::shared_ptr<Expr> child() const noexcept {
            return this->_child;
        }

        std::vector<std::string> columns() const noexcept {
            return this->_columns;
        }

        std::string column(std::size_t i) const {
            return this->_columns.at(i);
        }

        std::size_t numColumns() const noexcept {
            return this->_columns.size();
        }

    private:
        std::vector<std::string> _columns;
        std::shared_ptr<Expr> _child;
    };
}
