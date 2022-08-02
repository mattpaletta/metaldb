#include <cpptest/cpptest.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "RawTableCreator.hpp"

static metaldb::TempRow GenerateTempRow() {
    metaldb::TempRow::TempRowBuilder builder;
    builder.numColumns = 3;
    builder.columnTypes[0] = metaldb::ColumnType::Integer;
    builder.columnTypes[1] = metaldb::ColumnType::Integer;
    builder.columnTypes[2] = metaldb::ColumnType::Integer;

    metaldb::TempRow tempRow = builder;
    tempRow.Append((metaldb::types::IntegerType) 74);
    tempRow.Append((metaldb::types::IntegerType) 13);
    tempRow.Append((metaldb::types::IntegerType) 40);
    return tempRow;
}

class ProjectionInstructionTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(ProjectionInstructionTest)

NEW_TEST(ProjectionInstructionTest, SerializeProjectionInstruction) {
    using namespace metaldb;
    using namespace metaldb::engine;
    Projection projection({2, 0});

    Encoder encoder;
    encoder.encode(projection);
    auto buffer = encoder.data();

    CPPTEST_ASSERT(buffer.size() > 2);
    CPPTEST_ASSERT(buffer.at(0) == 1); // Size.
    CPPTEST_ASSERT((InstructionType) buffer.at(1) == InstructionType::PROJECTION);

    ProjectionInstruction projectionInst = &buffer.at(2);

    auto tempRow = GenerateTempRow();

    CPPTEST_ASSERT(projectionInst.NumColumns() == 2);
    {
        CPPTEST_ASSERT(projectionInst.GetColumnIndex(0) == 2);
        CPPTEST_ASSERT(projectionInst.GetColumnIndex(1) == 0);
    }
    CPPTEST_ASSERT(projectionInst.End() - &buffer.at(0) == buffer.size());
}

NEW_TEST(ProjectionInstructionTest, ReadProjectionInstruction) {
    using namespace metaldb;
    using namespace metaldb::engine;
    Projection projection({2, 0});

    Encoder encoder;
    encoder.encode(projection);
    auto buffer = encoder.data();

    CPPTEST_ASSERT(buffer.size() > 2);
    CPPTEST_ASSERT(buffer.at(0) == 1); // Size.
    CPPTEST_ASSERT((InstructionType) buffer.at(1) == InstructionType::PROJECTION);

    ProjectionInstruction projectionInst = &buffer.at(2);

    auto tempRow = GenerateTempRow();

    std::array<metaldb::OutputSerializedValue, 10'000> output;
    std::array<metaldb::OutputRow::NumBytesType, 10'000> scratch;
    metaldb::RawTable rawTable(nullptr);
    metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

    {
        constants.thread_position_in_threadgroup = 0;
        auto row = projectionInst.GetRow(tempRow, constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 40);
        CPPTEST_ASSERT(row.ReadColumnInt(1) == 74);
    }
}

CPPTEST_END_CLASS(ProjectionInstructionTest)
