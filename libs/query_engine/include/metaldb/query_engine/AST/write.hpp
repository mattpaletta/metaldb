#pragma once

#include "expr.hpp"
#include "method.h"

#include <vector>
#include <string>
#include <memory>

namespace metaldb::QueryEngine::AST {
    class Write final : public Expr {
    public:
        Write(std::string filepath, std::vector<std::string> columns, Method method, std::shared_ptr<Expr> child) : _filepath(std::move(filepath)), _columns(std::move(columns)), _method(method), _child(std::move(child)) {}
        ~Write() noexcept = default;

        std::string filepath() const noexcept {
            return this->_filepath;
        }

        Method method() const noexcept {
            return this->_method;
        }

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
        std::string _filepath;
        std::vector<std::string> _columns;
        Method _method;
        std::shared_ptr<Expr> _child;
    };
}
