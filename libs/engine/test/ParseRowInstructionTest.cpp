#include <cpptest/cpptest.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "RawTableCreator.hpp"

template<typename T>
bool ApproximatelyEqual(T a, T b) {
    return std::abs(a - b) < std::numeric_limits<T>::epsilon();
}

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

    CPPTEST_ASSERT(parseRowInst.GetMethod() == Method::CSV);
    CPPTEST_ASSERT(parseRowInst.NumColumns() == 5);
    {
        CPPTEST_ASSERT(parseRowInst.GetColumnType(0) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.GetColumnType(1) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.GetColumnType(2) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.GetColumnType(3) == ColumnType::Float);
        CPPTEST_ASSERT(parseRowInst.GetColumnType(4) == ColumnType::String);
    }
    CPPTEST_ASSERT(parseRowInst.SkipHeader() == false);
    CPPTEST_ASSERT(parseRowInst.End() - &buffer.at(0) == buffer.size());
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
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 0) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 1) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 2) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 3) == 1);

    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 1, 0) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 1, 1) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 1, 2) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 1, 3) == 1);


    std::array<metaldb::OutputSerializedValue, 10'000> output;
    std::array<metaldb::OutputRow::NumBytesType, 10'000> scratch;
    metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

    {
        constants.thread_position_in_threadgroup = 0;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 4);
        CPPTEST_ASSERT(row.ReadColumnInt(1) == 3);
        CPPTEST_ASSERT(row.ReadColumnInt(2) == 2);
        CPPTEST_ASSERT(row.ReadColumnInt(3) == 1);
    }
    {
        constants.thread_position_in_threadgroup = 1;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 5);
        CPPTEST_ASSERT(row.ReadColumnInt(1) == 6);
        CPPTEST_ASSERT(row.ReadColumnInt(2) == 7);
        CPPTEST_ASSERT(row.ReadColumnInt(3) == 8);
    }
}

NEW_TEST(ParseRowInstructionTest, ReadParseRowInstructionReverseOrder) {
    // Test that the random order cache works
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

    std::array<metaldb::OutputSerializedValue, 10'000> output;
    std::array<metaldb::OutputRow::NumBytesType, 10'000> scratch;
    metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

    {
        constants.thread_position_in_threadgroup = 0;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(3) == 1);
        CPPTEST_ASSERT(row.ReadColumnInt(2) == 2);
        CPPTEST_ASSERT(row.ReadColumnInt(1) == 3);
        CPPTEST_ASSERT(row.ReadColumnInt(0) == 4);
    }
    {
        constants.thread_position_in_threadgroup = 1;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(3) == 8);
        CPPTEST_ASSERT(row.ReadColumnInt(2) == 7);
        CPPTEST_ASSERT(row.ReadColumnInt(1) == 6);
        CPPTEST_ASSERT(row.ReadColumnInt(0) == 5);
    }
}

NEW_TEST(ParseRowInstructionTest, ReadParseRowInstructionString) {
    using namespace metaldb;
    using namespace metaldb::engine;
    ParseRow parseRow(Method::CSV, {ColumnType::Integer, ColumnType::String}, /* skipHeader */ false);

    Encoder encoder;
    encoder.encode(parseRow);
    auto buffer = encoder.data();

    auto serialized = [] {
        // Generate a csv and serialize it and read it with a RawTable object (CPU)
        auto [rawData, rowIndexes] = StringsToRow("4,\"abc\"", "5,\"def\"");
        std::vector<std::string> columns{"colA", "colB"};

        metaldb::reader::RawTable rawTableCPU{std::move(rawData), rowIndexes, columns};
        CPPTEST_ASSERT(rawTableCPU.NumRows() == 2);

        auto serialized = [&] {
            auto serialized = metaldb::Scheduler::SerializeRawTable(rawTableCPU, rawTableCPU.NumRows());
            CPPTEST_ASSERT(!serialized.empty());
            return *(serialized.at(0).first);
        }();
        CPPTEST_ASSERT(serialized.size() > rawTableCPU.data.size());
        CPPTEST_ASSERT(!rawTableCPU.data.empty());
        return serialized;
    }();
    metaldb::RawTable rawTable(serialized.data());

    CPPTEST_ASSERT(buffer.size() > 2);
    CPPTEST_ASSERT(buffer.at(0) == 1); // Size.
    CPPTEST_ASSERT((InstructionType) buffer.at(1) == InstructionType::PARSEROW);
    ParseRowInstruction parseRowInst = &buffer.at(2);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 0) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 1) == 3);

    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 1, 0) == 1);
    CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 1, 1) == 3);

    std::array<metaldb::OutputSerializedValue, 10'000> output;
    std::array<metaldb::OutputRow::NumBytesType, 10'000> scratch;
    metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

    {
        constants.thread_position_in_threadgroup = 0;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 4);
        CPPTEST_ASSERT(row.ReadColumnString(1).Str() == "abc");
    }
    {
        constants.thread_position_in_threadgroup = 1;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 5);
        CPPTEST_ASSERT(row.ReadColumnString(1).Str() == "def");
    }
}

NEW_TEST(ParseRowInstructionTest, TaxiRowRegression) {
    using namespace metaldb;
    using namespace metaldb::engine;
    ParseRow parseRow(Method::CSV, {ColumnType::Integer, ColumnType::String, ColumnType::String, ColumnType::String, ColumnType::Float, ColumnType::Integer, ColumnType::Integer, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float}, /* skipHeader */ false);

    Encoder encoder;
    encoder.encode(parseRow);
    auto buffer = encoder.data();

    auto serialized = [] {
        // Generate a csv and serialize it and read it with a RawTable object (CPU)
        auto [rawData, rowIndexes] = StringsToRow("2,2022-02-01 00:20:21,2022-02-01 00:24:30,N,1.0,43,238,1.0,1.16,5.5,0.5,0.5,1.02,0.0,,0.3,7.82,1.0,1.0,0.0",
            "2,2022-02-01 00:32:26,2022-02-01 00:35:31,N,1.0,166,24,1.0,0.57,4.5,0.5,0.5,0.0,0.0,,0.3,5.8,2.0,1.0,0.0");
        std::vector<std::string> columns{"colA", "colB"};

        metaldb::reader::RawTable rawTableCPU{std::move(rawData), rowIndexes, columns};
        CPPTEST_ASSERT(rawTableCPU.NumRows() == 2);

        auto serialized = [&] {
            auto serialized = metaldb::Scheduler::SerializeRawTable(rawTableCPU, rawTableCPU.NumRows());
            CPPTEST_ASSERT(!serialized.empty());
            return *(serialized.at(0).first);
        }();
        CPPTEST_ASSERT(serialized.size() > rawTableCPU.data.size());
        CPPTEST_ASSERT(!rawTableCPU.data.empty());
        return serialized;
    }();
    metaldb::RawTable rawTable(serialized.data());

    CPPTEST_ASSERT(buffer.size() > 2);
    CPPTEST_ASSERT(buffer.at(0) == 1); // Size.
    CPPTEST_ASSERT((InstructionType) buffer.at(1) == InstructionType::PARSEROW);
    ParseRowInstruction parseRowInst = &buffer.at(2);

    std::array<metaldb::OutputSerializedValue, 10'000> output;
    std::array<metaldb::OutputRow::NumBytesType, 10'000> scratch;
    metaldb::DbConstants constants{rawTable, output.data(), scratch.data()};

    {
        constants.thread_position_in_threadgroup = 0;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 2);
        CPPTEST_ASSERT(row.ReadColumnString(1).Str() == "2022-02-01 00:20:21");
        CPPTEST_ASSERT(row.ReadColumnString(2).Str() == "2022-02-01 00:24:30");
        CPPTEST_ASSERT(row.ReadColumnString(3).Str() == "N");
        CPPTEST_ASSERT(row.ReadColumnFloat(4) == 1.0);
        CPPTEST_ASSERT(row.ReadColumnInt(5) == 43);
        CPPTEST_ASSERT(row.ReadColumnInt(6) == 238);
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(7), 1.0f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(8), 1.16f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(9), 5.5f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(10), 0.5f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(11), 0.5f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(12), 1.02f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(13), 0.0f));
        CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 14) == 0);
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(15), 0.3f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(16), 7.82f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(17), 1.0f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(18), 1.0f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(19), 0.0f));
    }
    {
        constants.thread_position_in_threadgroup = 1;
        auto row = parseRowInst.GetRow(constants);

        CPPTEST_ASSERT(row.ReadColumnInt(0) == 2);
        CPPTEST_ASSERT(row.ReadColumnString(1).Str() == "2022-02-01 00:32:26");
        CPPTEST_ASSERT(row.ReadColumnString(2).Str() == "2022-02-01 00:35:31");
        CPPTEST_ASSERT(row.ReadColumnString(3).Str() == "N");
        CPPTEST_ASSERT(row.ReadColumnFloat(4) == 1.0);
        CPPTEST_ASSERT(row.ReadColumnInt(5) == 166);
        CPPTEST_ASSERT(row.ReadColumnInt(6) == 24);
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(7), 1.0f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(8), 0.57f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(9), 4.5f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(10), 0.5f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(11), 0.5f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(12), 0.0f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(13), 0.0f));
        CPPTEST_ASSERT(parseRowInst.ReadCSVColumnLength(rawTable, 0, 14) == 0);
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(15), 0.3f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(16), 5.8f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(17), 2.0f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(18), 1.0f));
        CPPTEST_ASSERT(ApproximatelyEqual(row.ReadColumnFloat(19), 0.0f));
    }
}


CPPTEST_END_CLASS(ParseRowInstructionTest)
