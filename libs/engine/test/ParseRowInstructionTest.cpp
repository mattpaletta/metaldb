#include <cpptest/cpptest.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "RawTableCreator.hpp"

class ParseRowInstructionTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(ParseRowInstructionTest)

NEW_TEST(ParseRowInstructionTest, SerializeParseRowInstruction) {
    using namespace metaldb;
    using namespace metaldb::engine;
    ParseRow parseRow(Method::CSV, {ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::String}, /* skipHeader */ false);

    Encoder encoder;
    encoder.encode(parseRow);
    auto buffer = encoder.data();

    CPPTEST_ASSERT(buffer.size() > 2);
    CPPTEST_ASSERT(buffer.at(0) == 1); // Size.
    CPPTEST_ASSERT((InstructionType) buffer.at(1) == InstructionType::PARSEROW);

    ParseRowInstruction parseRowInst = &buffer.at(2);

    CPPTEST_ASSERT(parseRowInst.getMethod() == Method::CSV);
    CPPTEST_ASSERT(parseRowInst.numColumns() == 5);
    {
        CPPTEST_ASSERT(parseRowInst.getColumnType(0) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.getColumnType(1) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.getColumnType(2) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.getColumnType(3) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.getColumnType(4) == ColumnType::String);
    }
    CPPTEST_ASSERT(parseRowInst.skipHeader() == false);
    CPPTEST_ASSERT(parseRowInst.end() - &buffer.at(0) == buffer.size());
}

NEW_TEST(ParseRowInstructionTest, ReadParseRowInstruction) {
    using namespace metaldb;
    using namespace metaldb::engine;
    ParseRow parseRow(Method::CSV, {ColumnType::Integer, ColumnType::Integer, ColumnType::Integer, ColumnType::Integer}, /* skipHeader */ false);

    Encoder encoder;
    encoder.encode(parseRow);
    auto buffer = encoder.data();

    auto serialized = CreateMetalRawTable();
    metaldb::RawTable rawTable(serialized.data());

    CPPTEST_ASSERT(buffer.size() > 2);
    CPPTEST_ASSERT(buffer.at(0) == 1); // Size.
    CPPTEST_ASSERT((InstructionType) buffer.at(1) == InstructionType::PARSEROW);
    ParseRowInstruction parseRowInst = &buffer.at(2);
    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 0, 0) == 1);
    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 0, 1) == 1);
    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 0, 2) == 1);
    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 0, 3) == 1);

    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 1, 0) == 1);
    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 1, 1) == 1);
    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 1, 2) == 1);
    CPPTEST_ASSERT(parseRowInst.readCSVColumnLength(rawTable, 1, 3) == 1);


    std::array<metaldb::OutputSerializedValue, 10'000> output;
    std::array<metaldb::OutputRow::NumBytesType, 10'000> scratch;
    metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

    {
        constants.thread_position_in_grid = 0;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 4);
        CPPTEST_ASSERT(row.ReadColumnInt(1) == 3);
        CPPTEST_ASSERT(row.ReadColumnInt(2) == 2);
        CPPTEST_ASSERT(row.ReadColumnInt(3) == 1);
    }
    {
        constants.thread_position_in_grid = 1;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 5);
        CPPTEST_ASSERT(row.ReadColumnInt(1) == 6);
        CPPTEST_ASSERT(row.ReadColumnInt(2) == 7);
        CPPTEST_ASSERT(row.ReadColumnInt(3) == 8);
    }
}

CPPTEST_END_CLASS(ParseRowInstructionTest)
