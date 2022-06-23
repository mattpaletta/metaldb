#include <cpptest/cpptest.hpp>
#include "RawTableCreator.hpp"

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
    CreateMetalRawTable();
}

NEW_TEST(RawTableTest, NullColumnTableTest) {
    // Generate a csv and serialize it and read it with a RawTable object (CPU)
    auto [rawData, rowIndexes] = StringsToRow("4,,2,1", "5,6,7,8", "9,10,,11");
    std::vector<std::string> columns{"colA", "colB", "colC", "colD"};

    metaldb::reader::RawTable rawTableCPU{std::move(rawData), rowIndexes, columns};
    CPPTEST_ASSERT(rawTableCPU.numRows() == 3);

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

    for (std::size_t i = 0; i < rawTableCPU.numRows(); ++i) {
        CPPTEST_ASSERT(metalRawTable.GetRowIndex(i) == rowIndexes.at(i));
    }

    {
        auto* data = metalRawTable.data();
        for (std::size_t i = 0; i < metalRawTable.GetSizeOfData(); ++i) {
            CPPTEST_ASSERT(data[i] == rawTableCPU.data.at(i));
        }
    }
}

CPPTEST_END_CLASS(RawTableTest)
