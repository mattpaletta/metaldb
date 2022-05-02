#include <metaldb/query_engine/query_engine.hpp>

auto metaldb::QueryEngine::QueryEngine::compile(const QueryInfo& query) -> QueryPlan {
    /**
     * Read list of files from table location
     * Determine schema of files (if not saved) [ pass in as object ]
     * Determine the columns to read (final)
     *    - if different than table, use a projection
     * Determine filters (row-wise) & if they are computed (based on transforms) or not [keep track of column dependency graph]
     * Determine transformations (using similar mechanism as filter)
     * The query plan should allow you to group files based on the plan they will run, and output a graph that can be traversed (like make) as it is submitted
     *      - Allow a limit per group
     */


    return QueryPlan();
}
