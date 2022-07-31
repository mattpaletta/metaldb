#include <metaldb/engine/Engine.hpp>
#include <metaldb/query_engine/query_engine.hpp>
#include <metaldb/query_engine/parser.hpp>

#include "engine.h"
#include "Scheduler.hpp"

auto metaldb::engine::Engine::runImpl() -> Dataframe {
    QueryEngine::Parser parser;
    QueryEngine::QueryEngine query;
    auto parseAst = parser.Parse("eg query.");
    {
        QueryEngine::TableDefinition taxiTable;
        taxiTable.name = "taxi";
        taxiTable.filePath = "../../datasets/taxi";
        {
            taxiTable.columns.emplace_back("VendorID",                  ColumnType::Integer);
            taxiTable.columns.emplace_back("lpep_pickup_datetime",  20, ColumnType::String);
            taxiTable.columns.emplace_back("lpep_dropoff_datetime", 20, ColumnType::String);
            taxiTable.columns.emplace_back("store_and_fwd_flag",    1,  ColumnType::String);
            taxiTable.columns.emplace_back("RatecodeID",                ColumnType::Float);
            taxiTable.columns.emplace_back("PULocationID",              ColumnType::Integer);
            taxiTable.columns.emplace_back("DOLocationID",              ColumnType::Integer);
            taxiTable.columns.emplace_back("passenger_count",           ColumnType::Float);
            taxiTable.columns.emplace_back("trip_distance",             ColumnType::Float);
            taxiTable.columns.emplace_back("fare_amount",               ColumnType::Float);
            taxiTable.columns.emplace_back("extra",                     ColumnType::Float);
            taxiTable.columns.emplace_back("mta_tax",                   ColumnType::Float);
            taxiTable.columns.emplace_back("tip_amount",                ColumnType::Float);
            taxiTable.columns.emplace_back("tolls_amount",              ColumnType::Float);
            taxiTable.columns.emplace_back("ehail_fee",                 ColumnType::Float, true);
            taxiTable.columns.emplace_back("improvement_surcharge",     ColumnType::Float);
            taxiTable.columns.emplace_back("total_amount",              ColumnType::Float);
            taxiTable.columns.emplace_back("payment_type",              ColumnType::Float);
            taxiTable.columns.emplace_back("trip_type",                 ColumnType::Float);
            taxiTable.columns.emplace_back("congestion_surcharge",      ColumnType::Float);
        }
        query.metadata.tables.push_back(taxiTable);

        taxiTable.name = "taxi_sample";
        taxiTable.filePath = "../../datasets/taxi_sample";
        query.metadata.tables.push_back(std::move(taxiTable));
    }
    {
        QueryEngine::TableDefinition irisTable;
        irisTable.name = "iris";
        irisTable.filePath = "../../datasets/iris";
        {
            irisTable.columns.emplace_back("sepal.length",      ColumnType::Float);
            irisTable.columns.emplace_back("sepal.width",       ColumnType::Float);
            irisTable.columns.emplace_back("petal.length",      ColumnType::Float);
            irisTable.columns.emplace_back("petal.width",       ColumnType::Float);
            irisTable.columns.emplace_back("variety",       10, ColumnType::String);
        }
        query.metadata.tables.push_back(std::move(irisTable));
    }
    auto plan = query.compile(parseAst);

    // Use 1 thread for now
    // Could easily expand to multiple in the future
    tf::Executor executor{1};
    auto taskflow = Scheduler::schedule(plan);
    auto future = executor.run(taskflow);
    future.wait();

    return Dataframe();
}
