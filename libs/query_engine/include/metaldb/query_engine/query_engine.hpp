#pragma once

#include "query_plan.hpp"
#include "query_info.hpp"

#include <string>

namespace metaldb::QueryEngine {
    class QueryEngine {
    public:
        QueryEngine() = default;

        ~QueryEngine() = default;

        QueryPlan compile(const QueryInfo& query);
    };
}
