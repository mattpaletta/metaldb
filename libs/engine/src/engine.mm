#include <metaldb/engine/Engine.hpp>
#include <metaldb/query_engine/query_engine.hpp>
#include <metaldb/query_engine/parser.hpp>

#include "engine.h"
#include "Scheduler.hpp"

#include <iostream>
#include <array>
#include <vector>

auto metaldb::engine::Engine::runImpl(const reader::RawTable& rawTable, instruction_serialized_type&& instructions) -> Dataframe {
    metaldb::QueryEngine::Parser parser;
    metaldb::QueryEngine::QueryEngine query;
    auto parseAst = parser.Parse("eg query.");
    {
        QueryEngine::TableDefinition taxiTable;
        taxiTable.name = "taxi";
        taxiTable.filePath = "../taxi";
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
        query.metadata.tables.push_back(std::move(taxiTable));
    }
    {
        QueryEngine::TableDefinition irisTable;
        irisTable.name = "iris";
        irisTable.filePath = "../iris";
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

//    auto manager = MetalManager::Create();
//    assert(manager);
//
//    auto rawDataSerialized = SerializeRawTable(rawTable);
//    std::array<int8_t, 1'000'000> outputBuffer{0};
//
//    manager->run(rawDataSerialized, instructions, outputBuffer, rawTable.numRows());
//
    // Read output buffer to Dataframe.
    Dataframe df;

    // Print to console
//    {
//        constexpr size_t HeaderOffset = 0;
//        const size_t sizeOfHeader = outputBuffer[HeaderOffset];
//        constexpr size_t NumBytesOffset = HeaderOffset + 1;
//        const size_t numBytes = *(uint32_t*)(&outputBuffer[NumBytesOffset]);
//        constexpr size_t NumColumnsOffset = NumBytesOffset + sizeof(uint32_t);
//        const size_t numColumns = outputBuffer[NumColumnsOffset];
//
//        std::cout << "Size of header: " << sizeOfHeader << std::endl;
//        std::cout << "Num bytes: " << numBytes << std::endl;
//        std::cout << "Num columns: " << numColumns << std::endl;
//
//        // Allocate this once so we don't change it
//        std::vector<std::size_t> columnSizes{numColumns, 0};
//        std::vector<ColumnType> columnTypes{numColumns, ColumnType::Unknown};
//        std::vector<size_t> variableLengthColumns;
//        for (size_t i = 0; i < numColumns; ++i) {
//            constexpr auto ColumnTypeStartOffset = NumColumnsOffset + 1;
//            const auto columnType = (ColumnType) outputBuffer[ColumnTypeStartOffset + i];
//            columnTypes.at(i) = columnType;
//            std::cout << "Column Type: " << i << " " << columnType << std::endl;
//
//            switch (columnType) {
//            case String:
//                columnSizes.at(i) = 0;
//                variableLengthColumns.push_back(i);
//                break;
//            case Float:
//                columnSizes.at(i) = sizeof(metaldb::types::FloatType);
//                break;
//            case Integer:
//                columnSizes.at(i) = sizeof(metaldb::types::IntegerType);
//                break;
//            case Unknown:
//                assert(false);
//                break;
//            }
//        }
//
//        // Read the row
//        size_t i = sizeOfHeader;
//        std::size_t rowNum = 0;
//        while (i < numBytes) {
//            std::cout << "Reading row: " << rowNum++ << std::endl;
//            // Read the column sizes for the dynamic sized ones
//            for (const auto& varLengthCol : variableLengthColumns) {
//                columnSizes[varLengthCol] = (std::size_t) outputBuffer[i++];
//            }
//
//            // Read the row
//            for (int col = 0; col < numColumns; ++col) {
//                auto columnType = columnTypes.at(col);
//                auto columnSize = columnSizes.at(col);
//
//                switch (columnType) {
//                case Integer: {
//                    auto* val = (types::IntegerType*) &outputBuffer[i];
//                    std::cout << "Reading Int: " << *val << " with size: " << columnSize << std::endl;
//                    break;
//                }
//                case Float: {
//                    auto* val = (types::FloatType*) &outputBuffer[i];
//                    std::cout << "Reading Float: " << *val << " with size: " << columnSize << std::endl;
//                    break;
//                }
//                case String: {
//                    std::string temp;
//                    for (std::size_t j = 0; j < columnSize; ++j) {
//                        temp += outputBuffer[i + j];
//                    }
//
//                    std::cout << "Reading String: " << temp << " with size: " << columnSize << std::endl;
//                    break;
//                }
//                case Unknown:
//                    std::cout << "Unsupported Column" << std::endl;
//                    break;
//                }
//
//                i += columnSize;
//            }
//        }
//    }

    return df;
}
