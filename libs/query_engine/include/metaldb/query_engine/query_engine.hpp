#pragma once

#include "AST/expr.hpp"
#include "metadata.hpp"
#include "query_plan.hpp"

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
