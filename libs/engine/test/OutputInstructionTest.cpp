#include <cpptest/cpptest.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "RawTableCreator.hpp"
#include "OutputRowReader.hpp"
#include "OutputRowWriter.hpp"

static metaldb::TempRow GenerateTempRow(std::size_t i) {
    metaldb::TempRow::TempRowBuilder builder;
    builder.numColumns = 3;
    builder.columnTypes[0] = metaldb::ColumnType::Integer;
    builder.columnTypes[1] = metaldb::ColumnType::Integer;
    builder.columnTypes[2] = metaldb::ColumnType::Integer;

    metaldb::TempRow tempRow = builder;
    tempRow.Append((metaldb::types::IntegerType) (74 + i));
    tempRow.Append((metaldb::types::IntegerType) (13 + i));
    tempRow.Append((metaldb::types::IntegerType) (40 + i));
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
    CPPTEST_ASSERT(outputInst.End() - &buffer.at(0) == buffer.size());
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
        constants.thread_position_in_threadgroup = 0;
        auto tempRow = GenerateTempRow(0);
        outputInst.WriteRow(tempRow, constants);
    }
    {
        constants.thread_position_in_threadgroup = 1;
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

NEW_TEST(OutputInstructionTest, MultipleWriters) {
    using namespace metaldb;
    using namespace metaldb::engine;
    OutputInstruction outputInst = nullptr;

    std::array<metaldb::OutputSerializedValue, 100'000> output0{0};
    std::array<metaldb::OutputSerializedValue, 100'000> output1{0};
    auto generateReader = [&](std::size_t count, auto& output) {
        std::array<metaldb::OutputRow::NumBytesType, 500> scratch{0};
        metaldb::RawTable rawTable(nullptr);
        metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

        for (std::size_t i = 0; i < count; ++i) {
            constants.thread_position_in_threadgroup = i;
            auto tempRow = GenerateTempRow(i);
            outputInst.WriteRow(tempRow, constants);
        }

        auto reader = metaldb::OutputRowReader(output);
        CPPTEST_ASSERT(reader.NumRows() == count);
        CPPTEST_ASSERT(reader.NumColumns() == 3);
        {
            CPPTEST_ASSERT(reader.TypeOfColumn(0) == ColumnType::Integer);
            CPPTEST_ASSERT(reader.TypeOfColumn(1) == ColumnType::Integer);
            CPPTEST_ASSERT(reader.TypeOfColumn(2) == ColumnType::Integer);
        }

        return reader;
    };

    auto reader0 = generateReader(2000, output0);
    auto reader1 = generateReader(3000, output1);

    metaldb::OutputRowWriter writer2;
    for (std::size_t i = 0; i < reader0.NumRows(); ++i) {
        writer2.copyRow(reader0, i);
    }
    for (std::size_t i = 0; i < reader1.NumRows(); ++i) {
        writer2.copyRow(reader1, i);
    }
    CPPTEST_ASSERT(writer2.NumColumns() == reader0.NumColumns());
    CPPTEST_ASSERT(writer2.CurrentNumRows() == reader0.NumRows() + reader1.NumRows());
}

NEW_TEST(OutputInstructionTest, ReadWriteRead) {
    using namespace metaldb;
    using namespace metaldb::engine;
    OutputInstruction outputInst = nullptr;

    std::array<metaldb::OutputSerializedValue, 100'000> output0{0};
    auto generateReader = [&](std::size_t count, auto& output) {
        std::array<metaldb::OutputRow::NumBytesType, 500> scratch{0};
        metaldb::RawTable rawTable(nullptr);
        metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

        for (std::size_t i = 0; i < count; ++i) {
            constants.thread_position_in_threadgroup = i;
            auto tempRow = GenerateTempRow(i);
            outputInst.WriteRow(tempRow, constants);
        }

        auto reader = metaldb::OutputRowReader(output);
        CPPTEST_ASSERT(reader.NumRows() == count);
        CPPTEST_ASSERT(reader.NumColumns() == 3);
        {
            CPPTEST_ASSERT(reader.TypeOfColumn(0) == ColumnType::Integer);
            CPPTEST_ASSERT(reader.TypeOfColumn(1) == ColumnType::Integer);
            CPPTEST_ASSERT(reader.TypeOfColumn(2) == ColumnType::Integer);
        }

        return reader;
    };

    auto reader0 = generateReader(2000, output0);
    metaldb::OutputRowWriter writer1;
    for (std::size_t i = 0; i < reader0.NumRows(); ++i) {
        writer1.copyRow(reader0, i);
    }
    std::vector<metaldb::OutputRowReader<>::value_type> instructions0;
    writer1.write(instructions0);

    CPPTEST_ASSERT(writer1.NumColumns() == reader0.NumColumns());
    CPPTEST_ASSERT(writer1.CurrentNumRows() == reader0.NumRows());
    CPPTEST_ASSERT(writer1.SizeOfHeader() == reader0.SizeOfHeader());
    CPPTEST_ASSERT(writer1.NumBytes() == reader0.NumBytes());

    auto reader1 = metaldb::OutputRowReader(instructions0);

    CPPTEST_ASSERT(reader0.NumColumns() == reader1.NumColumns());
    CPPTEST_ASSERT(reader0.NumRows() == reader1.NumRows());
    CPPTEST_ASSERT(reader0.NumBytes() == reader1.NumBytes());

    for (std::size_t i = 0; i < instructions0.size(); ++i) {
        CPPTEST_ASSERT(instructions0.at(i) == output0.at(i));
    }
}


CPPTEST_END_CLASS(OutputInstructionTest)
