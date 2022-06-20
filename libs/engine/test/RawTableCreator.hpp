//
//  RawTableCreator.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-06-11.
//

#pragma once

#include <metaldb/reader/RawTable.hpp>

#include "raw_table.h"
#include "Scheduler.hpp"

#include <vector>

template<typename... Args>
static std::pair<std::vector<char>, std::vector<metaldb::RawTable::RowIndexType>> StringsToRow(Args... input) {
    std::vector<char> output;
    std::vector<metaldb::RawTable::RowIndexType> rowIndexes;
    for (std::string str : {input...}) {
        rowIndexes.push_back(output.size());
        for (auto c : str) {
            output.push_back(c);
        }
    }
    return std::make_pair(output, rowIndexes);
}

static std::vector<char> CreateMetalRawTable() {
    // Generate a csv and serialize it and read it with a RawTable object (CPU)
    auto [rawData, rowIndexes] = StringsToRow("4,3,2,1", "5,6,7,8");
    std::vector<std::string> columns{"colA", "colB", "colC", "colD"};

    metaldb::reader::RawTable rawTableCPU{std::move(rawData), rowIndexes, columns};
    CPPTEST_ASSERT(rawTableCPU.numRows() == 2);

    auto serialized = [&] {
        auto serialized = metaldb::Scheduler::SerializeRawTable(rawTableCPU, rawTableCPU.numRows());
        CPPTEST_ASSERT(!serialized.empty());
        return *(serialized.at(0).first);
    }();
    CPPTEST_ASSERT(serialized.size() > rawTableCPU.data.size());
    CPPTEST_ASSERT(!rawTableCPU.data.empty());

    auto metalRawTable = metaldb::RawTable(serialized.data());
    CPPTEST_ASSERT(metalRawTable.GetSizeOfHeader() > 0);
    CPPTEST_ASSERT(metalRawTable.GetSizeOfHeader() < 100);

    CPPTEST_ASSERT(metalRawTable.GetStartOfData() > 0);
    CPPTEST_ASSERT(metalRawTable.GetStartOfData() < 1000);
    CPPTEST_ASSERT(metalRawTable.GetSizeOfData() == rawTableCPU.data.size());
    CPPTEST_ASSERT(metalRawTable.GetNumRows() == rawTableCPU.numRows());

    CPPTEST_ASSERT(metalRawTable.GetRowIndex(0) == 0);
    CPPTEST_ASSERT(metalRawTable.GetRowIndex(1) == 7);

    {
        auto* data = metalRawTable.data();
        for (std::size_t i = 0; i < metalRawTable.GetSizeOfData(); ++i) {
            CPPTEST_ASSERT(data[i] == rawTableCPU.data.at(i));
        }
    }

    return serialized;
}
