#pragma once

#include "query_plan.hpp"
#include "metadata.hpp"
#include "AST/expr.hpp"

#include <memory>

namespace metaldb::QueryEngine {
    class QueryEngine {
    public:
        QueryEngine() = default;

        ~QueryEngine() = default;

        QueryPlan compile(std::shared_ptr<AST::Expr> expr);

        Metadata metadata;
    };
}
