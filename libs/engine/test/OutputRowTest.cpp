#include <cpptest/cpptest.hpp>

#include <metaldb/engine/Instructions.hpp>

#include "OutputRowReader.hpp"
#include "OutputRowWriter.hpp"

#include "output_instruction.h"
#include "temp_row.h"

#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <array>

static metaldb::TempRow GenerateTempRow() {
    srand(123);

    metaldb::TempRow::TempRowBuilder builder;
    builder.numColumns = 3;
    builder.columnTypes[0] = metaldb::ColumnType::Integer;
    builder.columnTypes[1] = metaldb::ColumnType::Float;
    builder.columnTypes[2] = metaldb::ColumnType::Integer;

    metaldb::TempRow tempRow = builder;
    tempRow.append((metaldb::types::IntegerType) rand() % 74);
    tempRow.append((metaldb::types::FloatType) (rand() % 13) + .5f);
    tempRow.append((metaldb::types::IntegerType) rand() % 40);
    return tempRow;
}

static metaldb::TempRow GenerateNullTempRow() {
    srand(123);

    metaldb::TempRow::TempRowBuilder builder;
    builder.numColumns = 3;
    builder.columnTypes[0] = metaldb::ColumnType::Integer;
    builder.columnTypes[1] = metaldb::ColumnType::Float_opt;
    builder.columnSizes[1] = 0;
    builder.columnTypes[2] = metaldb::ColumnType::Integer;


    metaldb::TempRow tempRow = builder;
    tempRow.append((metaldb::types::IntegerType) rand() % 74);
//    tempRow.append((metaldb::types::FloatType) (rand() % 13) + .5f);
    tempRow.append((metaldb::types::IntegerType) rand() % 40);
    return tempRow;
}

static metaldb::OutputRowWriter WriterFromTempRows(size_t num) {
    metaldb::OutputRowWriter writer;
    for (std::size_t i = 0; i < num; ++i) {
        writer.appendTempRow(GenerateTempRow());
    }
    return writer;
}

static metaldb::OutputRowWriter WriterFromTempRowsWithNull(size_t num) {
    metaldb::OutputRowWriter writer;
    for (std::size_t i = 0; i < num; ++i) {
        writer.appendTempRow(GenerateNullTempRow());
    }
    return writer;
}

class OutputRowTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(OutputRowTest)

NEW_TEST(OutputRowTest, TempRowToOutputWriter) {
    auto tempRow = GenerateTempRow();

    metaldb::OutputRowWriter writer;
    writer.appendTempRow(tempRow);
    CPPTEST_ASSERT(writer.CurrentNumRows() == 1);
    CPPTEST_ASSERT(writer.NumColumns() == tempRow.NumColumns());
}

NEW_TEST(OutputRowTest, TempRowToOutputWriterMultiple) {
    auto tempRow = GenerateTempRow();
    auto tempRow2 = GenerateTempRow();
    auto tempRow3 = GenerateTempRow();

    metaldb::OutputRowWriter writer;
    writer.appendTempRow(tempRow);
    writer.appendTempRow(tempRow2);
    writer.appendTempRow(tempRow3);

    CPPTEST_ASSERT(writer.CurrentNumRows() == 3);
    CPPTEST_ASSERT(writer.NumColumns() == tempRow.NumColumns());
}

NEW_TEST(OutputRowTest, OutputRowToWriterToReader) {
    auto writer = WriterFromTempRows(10);

    std::vector<metaldb::OutputRowReader<>::value_type> instructions;
    writer.write(instructions);

    auto reader = metaldb::OutputRowReader(instructions);
    CPPTEST_ASSERT(reader.NumColumns() == writer.NumColumns());
    CPPTEST_ASSERT(reader.NumRows() == writer.CurrentNumRows());
}

NEW_TEST(OutputRowTest, OutputRowToWriterToReaderLarge) {
    auto writer = WriterFromTempRows(200'000);

    std::vector<metaldb::OutputRowReader<>::value_type> instructions;
    writer.write(instructions);

    auto reader = metaldb::OutputRowReader(instructions);
    CPPTEST_ASSERT(reader.NumColumns() == writer.NumColumns());
    CPPTEST_ASSERT(reader.NumRows() == writer.CurrentNumRows());
}

NEW_TEST(OutputRowTest, MultipleWritersMergeTest) {
    auto writer0 = WriterFromTempRows(20'000);
    std::vector<metaldb::OutputRowReader<>::value_type> instructions0;
    writer0.write(instructions0);
    auto reader0 = metaldb::OutputRowReader(instructions0);
    CPPTEST_ASSERT(reader0.NumColumns() == writer0.NumColumns());
    CPPTEST_ASSERT(reader0.NumRows() == writer0.CurrentNumRows());

    auto writer1 = WriterFromTempRows(30'000);
    std::vector<metaldb::OutputRowReader<>::value_type> instructions1;
    writer1.write(instructions1);
    auto reader1 = metaldb::OutputRowReader(instructions1);
    CPPTEST_ASSERT(reader1.NumColumns() == writer1.NumColumns());
    CPPTEST_ASSERT(reader1.NumRows() == writer1.CurrentNumRows());

    metaldb::OutputRowWriter writer2;
    for (std::size_t i = 0; i < reader0.NumRows(); ++i) {
        writer2.copyRow(reader0, i);
    }
    for (std::size_t i = 0; i < reader1.NumRows(); ++i) {
        writer2.copyRow(reader1, i);
    }
    CPPTEST_ASSERT(writer2.NumColumns() == writer0.NumColumns());
    CPPTEST_ASSERT(writer2.NumBytesData() == writer1.NumBytesData() + writer0.NumBytesData());
    CPPTEST_ASSERT(writer2.SizeOfHeader() >= writer1.SizeOfHeader());
    CPPTEST_ASSERT(writer2.SizeOfHeader() >= writer0.SizeOfHeader());
    CPPTEST_ASSERT(writer2.CurrentNumRows() == writer1.CurrentNumRows() + writer0.CurrentNumRows());
}

NEW_TEST(OutputRowTest, MultipleWritersMergeTestNull) {
    auto writer0 = WriterFromTempRowsWithNull(5);
    std::vector<metaldb::OutputRowReader<>::value_type> instructions0;
    writer0.write(instructions0);
    auto reader0 = metaldb::OutputRowReader(instructions0);
    CPPTEST_ASSERT(reader0.NumColumns() == writer0.NumColumns());
    CPPTEST_ASSERT(reader0.NumRows() == writer0.CurrentNumRows());

    auto writer1 = WriterFromTempRowsWithNull(7);
    std::vector<metaldb::OutputRowReader<>::value_type> instructions1;
    writer1.write(instructions1);
    auto reader1 = metaldb::OutputRowReader(instructions1);
    CPPTEST_ASSERT(reader1.NumColumns() == writer1.NumColumns());
    CPPTEST_ASSERT(reader1.NumRows() == writer1.CurrentNumRows());

    metaldb::OutputRowWriter writer2;
    for (std::size_t i = 0; i < reader0.NumRows(); ++i) {
        writer2.copyRow(reader0, i);
    }
    for (std::size_t i = 0; i < reader1.NumRows(); ++i) {
        writer2.copyRow(reader1, i);
    }
    CPPTEST_ASSERT(writer2.NumColumns() == writer0.NumColumns());
    CPPTEST_ASSERT(writer2.CurrentNumRows() == writer1.CurrentNumRows() + writer0.CurrentNumRows());
}

NEW_TEST(OutputRowTest, ReadWriteRead) {
    auto writer0 = WriterFromTempRows(300);
    std::vector<metaldb::OutputRowReader<>::value_type> instructions0;
    writer0.write(instructions0);
    auto reader0 = metaldb::OutputRowReader(instructions0);
    CPPTEST_ASSERT(reader0.NumColumns() == writer0.NumColumns());
    CPPTEST_ASSERT(reader0.NumRows() == writer0.CurrentNumRows());

    metaldb::OutputRowWriter writer1;
    for (std::size_t i = 0; i < reader0.NumRows(); ++i) {
        writer1.copyRow(reader0, i);
    }

    std::vector<metaldb::OutputRowReader<>::value_type> instructions1;
    writer1.write(instructions1);
    auto reader1 = metaldb::OutputRowReader(instructions1);
    CPPTEST_ASSERT(reader0.NumColumns() == reader1.NumColumns());
    CPPTEST_ASSERT(reader0.NumRows() == reader1.NumRows());
    CPPTEST_ASSERT(reader0.NumBytes() == reader1.NumBytes());

    CPPTEST_ASSERT(instructions0 == instructions1);
}

NEW_TEST(OutputRowTest, CompareHeaderSizeInstructionWriter) {
    auto row0 = GenerateTempRow();
    auto row1 = GenerateTempRow();
    auto row2 = GenerateTempRow();
    size_t sizeFromWriter = 0;
    size_t sizeFromInstruction = 0;
    {
        metaldb::OutputRowWriter writer;

        std::vector<metaldb::OutputRowReader<>::value_type> output;

        writer.appendTempRow(row0);
        writer.appendTempRow(row1);
        writer.appendTempRow(row2);

        writer.write(output);

        sizeFromWriter = metaldb::OutputRowReader<>::SizeOfHeader(output);
    }

    {
        using namespace metaldb;
        using namespace metaldb::engine;
        OutputInstruction outputInst = nullptr;

        std::array<metaldb::OutputSerializedValue, 200> output{0};
        std::array<metaldb::OutputRow::NumBytesType, 200> scratch{0};
        metaldb::RawTable rawTable(nullptr);
        metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

        {
            constants.thread_position_in_grid = 0;
            outputInst.WriteRow(row0, constants);
        }
        {
            constants.thread_position_in_grid = 1;
            outputInst.WriteRow(row1, constants);
        }
        {
            constants.thread_position_in_grid = 2;
            outputInst.WriteRow(row2, constants);
        }
        sizeFromInstruction = metaldb::OutputRowReader<decltype(output)>::SizeOfHeader(output);
    }
    CPPTEST_ASSERT(sizeFromWriter == sizeFromInstruction);
}

CPPTEST_END_CLASS(OutputRowTest)
