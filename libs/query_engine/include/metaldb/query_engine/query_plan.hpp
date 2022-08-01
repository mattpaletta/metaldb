#pragma once

#include "partials.hpp"

namespace metaldb::QueryEngine {
    class QueryPlan {
    public:
        QueryPlan() = default;
        ~QueryPlan() noexcept = default;

        std::vector<std::shared_ptr<Stage>> stages;
    };
}
