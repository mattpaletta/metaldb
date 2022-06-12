#include <cpptest/cpptest.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "RawTableCreator.hpp"
#include "OutputRowReader.hpp"

static metaldb::TempRow GenerateTempRow(std::size_t i) {
    metaldb::TempRow::TempRowBuilder builder;
    builder.numColumns = 3;
    builder.columnTypes[0] = metaldb::ColumnType::Integer;
    builder.columnTypes[1] = metaldb::ColumnType::Integer;
    builder.columnTypes[2] = metaldb::ColumnType::Integer;

    metaldb::TempRow tempRow = builder;
    tempRow.append((metaldb::types::IntegerType) (74 + i));
    tempRow.append((metaldb::types::IntegerType) (13 + i));
    tempRow.append((metaldb::types::IntegerType) (40 + i));
    return tempRow;
}

class OutputInstructionTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(OutputInstructionTest)

NEW_TEST(OutputInstructionTest, SerializeOutputInstruction) {
    using namespace metaldb;
    using namespace metaldb::engine;
    Output output;

    Encoder encoder;
    encoder.encode(output);
    auto buffer = encoder.data();

    CPPTEST_ASSERT(buffer.size() > 1);
    CPPTEST_ASSERT(buffer.at(0) == 1); // Size.
    CPPTEST_ASSERT((InstructionType) buffer.at(1) == InstructionType::OUTPUT);

    OutputInstruction outputInst = &buffer.at(1);
    CPPTEST_ASSERT(outputInst.end() - &buffer.at(0) == buffer.size());
}

NEW_TEST(OutputInstructionTest, ReadOutputInstruction) {
    using namespace metaldb;
    using namespace metaldb::engine;
    OutputInstruction outputInst = nullptr;

    std::array<metaldb::OutputSerializedValue, 200> output{0};
    std::array<metaldb::OutputRow::NumBytesType, 200> scratch{0};
    metaldb::RawTable rawTable(nullptr);
    metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

    {
        constants.thread_position_in_grid = 0;
        auto tempRow = GenerateTempRow(0);
        outputInst.WriteRow(tempRow, constants);
    }
    {
        constants.thread_position_in_grid = 1;
        auto tempRow = GenerateTempRow(1);
        outputInst.WriteRow(tempRow, constants);
    }

    auto reader = metaldb::OutputRowReader(output);
    CPPTEST_ASSERT(reader.NumRows() == 2);
    CPPTEST_ASSERT(reader.NumColumns() == 3);
    {
        CPPTEST_ASSERT(reader.TypeOfColumn(0) == ColumnType::Integer);
        CPPTEST_ASSERT(reader.TypeOfColumn(1) == ColumnType::Integer);
        CPPTEST_ASSERT(reader.TypeOfColumn(2) == ColumnType::Integer);
    }
}

CPPTEST_END_CLASS(OutputInstructionTest)
