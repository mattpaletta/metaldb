//
//  write.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-06-03.
//

#pragma once

#include "expr.hpp"
#include "method.h"

#include <vector>
#include <string>
#include <memory>

namespace metaldb::QueryEngine::AST {
    class Write final : public Expr {
    public:
        Write(const std::string& filepath, const std::vector<std::string>& columns, Method method, std::shared_ptr<Expr> child) : _filepath(filepath), _columns(columns), _method(method), _child(child) {}
        ~Write() = default;

        std::string filepath() const {
            return this->_filepath;
        }

        Method method() const {
            return this->_method;
        }

        bool hasChild() const {
            return this->child().operator bool();
        }

        std::shared_ptr<Expr> child() const {
            return this->_child;
        }

        std::vector<std::string> columns() const {
            return this->_columns;
        }

        std::string column(std::size_t i) const {
            return this->_columns.at(i);
        }

        std::size_t numColumns() const {
            return this->_columns.size();
        }

    private:
        std::string _filepath;
        std::vector<std::string> _columns;
        Method _method;
        std::shared_ptr<Expr> _child;
    };
}
