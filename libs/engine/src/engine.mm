#include <metaldb/engine/Engine.hpp>
#include <metaldb/query_engine/query_engine.hpp>
#include <metaldb/query_engine/parser.hpp>

#include "engine.h"
#include "Scheduler.hpp"

#include <iostream>
#include <array>
#include <vector>

auto metaldb::engine::Engine::runImpl() -> Dataframe {
    metaldb::QueryEngine::Parser parser;
    metaldb::QueryEngine::QueryEngine query;
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

    tf::Executor executor(1);
    auto taskflow = Scheduler::schedule(plan);
    auto future = executor.run(taskflow);
    future.wait();

    return Dataframe();

//    {
//        std::filesystem::path path;
//        path.append("../../datasets/iris/iris.csv");
//        reader::CSVReader reader(path);
//        std::cout << "Table Is Valid: " << (reader.isValid() ? "YES" : "NO") << std::endl;
//
//        reader::CSVReader::CSVOptions options;
//        options.containsHeaderLine = true;
//        options.stripQuotesFromHeader = true;
//        auto rawTable = reader.read(options);
//
//        std::cout << rawTable.debugStr() << std::endl;
//
//        auto manager = MetalManager::Create();
//        assert(manager);
//
//        auto rawDataSerialized = Scheduler::SerializeRawTable(rawTable, 1000000).at(0).first;
//        MetalManager::OutputBufferType outputBuffer;
//
//        engine::ParseRow parseRow(Method::CSV, {ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::String}, /* skipHeader */ false);
//        engine::Projection projection({0, 1});
//        engine::Output output;
//
//        Encoder encoder;
//        encoder.encodeAll(parseRow, projection, output);
//        manager->run(*rawDataSerialized, encoder.data(), outputBuffer, rawTable.numRows());
//    }
//
//    // Read output buffer to Dataframe.
//    Dataframe df;
    return Dataframe();
}
