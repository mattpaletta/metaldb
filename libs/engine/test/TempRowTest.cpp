#include <cpptest/cpptest.hpp>

#include "temp_row.h"

#include <iostream>

class TempRowTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(TempRowTest)

NEW_TEST(TempRowTest, TempRowToOutputWriter) {
    using namespace metaldb;
    TempRow::TempRowBuilder builder;
    builder.numColumns = 3;
    builder.columnTypes[0] = ColumnType::Integer;
    builder.columnTypes[1] = ColumnType::Integer;
    builder.columnTypes[2] = ColumnType::Integer;

    TempRow tempRow = builder;
    tempRow.append((types::IntegerType) 10);
    tempRow.append((types::IntegerType) 11);
    tempRow.append((types::IntegerType) 12);

    CPPTEST_ASSERT(tempRow.LengthOfHeader() > 0);
    CPPTEST_ASSERT(tempRow.LengthOfHeader() < 100);

    CPPTEST_ASSERT(tempRow.SizeOfPartialRow() > 0);
    CPPTEST_ASSERT(tempRow.SizeOfPartialRow() < 100);

    CPPTEST_ASSERT(tempRow.NumColumns() == 3);
    {
        CPPTEST_ASSERT(tempRow.ColumnType(0) == ColumnType::Integer);
        CPPTEST_ASSERT(tempRow.ColumnType(1) == ColumnType::Integer);
        CPPTEST_ASSERT(tempRow.ColumnType(2) == ColumnType::Integer);
    }
    {
        CPPTEST_ASSERT(!tempRow.ColumnVariableSize(0));
        CPPTEST_ASSERT(!tempRow.ColumnVariableSize(1));
        CPPTEST_ASSERT(!tempRow.ColumnVariableSize(2));
    }
    {
        CPPTEST_ASSERT(tempRow.ColumnSize(0) > 0);
        CPPTEST_ASSERT(tempRow.ColumnSize(1) > 0);
        CPPTEST_ASSERT(tempRow.ColumnSize(2) > 0);
    }
    {
        CPPTEST_ASSERT(tempRow.ReadColumnInt(0) == 10);
        CPPTEST_ASSERT(tempRow.ReadColumnInt(1) == 11);
        CPPTEST_ASSERT(tempRow.ReadColumnInt(2) == 12);
    }
    CPPTEST_ASSERT(tempRow.size() >= 3);
}

CPPTEST_END_CLASS(TempRowTest)
