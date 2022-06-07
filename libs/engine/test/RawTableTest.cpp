#include <cpptest/cpptest.hpp>
#include <metaldb/reader/RawTable.hpp>

#include "raw_table.h"
#include "Scheduler.hpp"

#include <vector>

class RawTableTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(RawTableTest)

NEW_TEST(RawTableTest, TempRowToOutputWriter) {
    // Generate a csv and serialize it and read it with a RawTable object (CPU)
    std::vector<char> rawData = {'4', ',', '3', ',', '2', ',', '1', '\n',
                                 '5', ',', '6', ',', '7', ',', '8'};
    std::vector<uint16_t> rowIndexes = {0, 8};
    std::vector<std::string> columns{"col"};

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
    CPPTEST_ASSERT(metalRawTable.GetStartOfData() > 0);
    CPPTEST_ASSERT(metalRawTable.GetSizeOfData() == rawTableCPU.data.size());
    CPPTEST_ASSERT(metalRawTable.GetNumRows() == rawTableCPU.numRows());

    {
        auto* data = metalRawTable.data();
        for (std::size_t i = 0; i < metalRawTable.GetSizeOfData(); ++i) {
            CPPTEST_ASSERT(data[i] == rawTableCPU.data.at(i));
        }
    }
}

CPPTEST_END_CLASS(RawTableTest)
