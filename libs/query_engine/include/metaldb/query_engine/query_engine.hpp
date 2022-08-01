#pragma once

#include "query_plan.hpp"
#include "metadata.hpp"
#include "AST/expr.hpp"

#include <memory>

namespace metaldb::QueryEngine {
    class QueryEngine {
    public:
        QueryEngine() = default;

        ~QueryEngine() noexcept = default;

        QueryPlan compile(const std::shared_ptr<AST::Expr>& expr) const;

        Metadata metadata;
    };
}
