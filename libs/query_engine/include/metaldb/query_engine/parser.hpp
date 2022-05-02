#pragma once

#include "AST/expr.hpp"

#include <memory>
#include <string>

namespace metaldb::QueryEngine {
    class Parser final {
    public:
        Parser() = default;
        ~Parser() = default;

        std::shared_ptr<AST::Expr> Parse(const std::string& query) const;
    };
}
