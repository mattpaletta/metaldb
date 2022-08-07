#pragma once

#include "constants.h"
#include "column_type.h"
#include "raw_table.h"
#include "db_constants.h"
#include "temp_row.h"

namespace metaldb {
    class ProjectionInstruction {
    public:
        /**
         * The type to store the number of columns in the row.
         */
        using NumColumnsType = OutputRow::NumColumnsType;
        
        /**
         * The starting offset of the number of columns in the row.
         */
        METAL_CONSTANT static constexpr auto NumColumnsOffset = 0;
        
        /**
         * The type to store the index of the column.
         * The index refers to which columns to read from the original row.
         * This is set to be the same as the @b NumColumnsType because they must be compatible.
         */
        using ColumnIndexType = NumColumnsType;
        
        /**
         * The starting offset for the column index.  This immediately suceeds the @b NumColumns value.
         */
        METAL_CONSTANT static constexpr auto ColumnIndexOffset = sizeof(NumColumnsType) + NumColumnsOffset;
        
#ifndef __METAL__
        ProjectionInstruction() CPP_NOEXCEPT : _instructions(nullptr) {}
#endif
        ProjectionInstruction(InstSerializedValuePtr instructions) CPP_NOEXCEPT : _instructions(instructions) {}
        
        NumColumnsType NumColumns() const CPP_NOEXCEPT {
            return ReadBytesStartingAt<NumColumnsType>(&this->_instructions[NumColumnsOffset]);
        }
        
        /**
         * Given an index, this returns which column to read at that index.
         * For example, in the query: `SELECT c, a FROM table;`
         * where the original table has columns a, b, c
         *        The projection would read [2, 0]
         *         Passing in the value 0, would return 2.
         *         Passing in the value 1, would return 0.
         *
         * @param index The index of the column to write to the @b OutputRow
         * @return The column to read from the input row
         */
        ColumnIndexType GetColumnIndex(NumColumnsType index) const CPP_NOEXCEPT {
            return ReadBytesStartingAt<ColumnIndexType>(&this->_instructions[(index * sizeof(ColumnIndexType)) + ColumnIndexOffset]);
        }
        
        /**
         * Returns a pointer 1 past the end of the projection instruction.  This will either be an unknown if we exceed the end of the array or
         * an encoded @b InstructionType .
         */
        InstSerializedValuePtr End() const CPP_NOEXCEPT {
            // Returns 1 past the end of the instruction
            const auto numColumns = this->NumColumns();
            const uint8_t offset = ColumnIndexOffset + (numColumns * sizeof(ColumnIndexType));
            return &this->_instructions[offset];
        }
        
        /**
         * Returns the ith row, based on the the thread number in @b constants as a @b TempRow .
         *
         * @param row The input row upon which to do the projection.
         * @param constants The struct of constants to use as temporary storage.
         */
        TempRow GetRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) CPP_NOEXCEPT {
            // Do the projection
            TempRow::TempRowBuilder builder;
            auto numCols = this->NumColumns();
            {
                builder.numColumns = numCols;
                
                // Read column types
                for (auto i = 0; i < numCols; ++i) {
                    auto columnToRead = this->GetColumnIndex(i);
                    
                    // Set all column types
                    auto columnType = row.ColumnType(columnToRead);
                    builder.columnTypes[i] = columnType;
                    
                    // Set all column sizes, and they might get pruned
                    builder.columnSizes[i] = row.ColumnSize(columnToRead);
                }
            }
            TempRow newRow = builder;
            // Copy the columns we are interested in
            for (auto i = 0; i < numCols; ++i) {
                const auto columnToRead = this->GetColumnIndex(i);
                
                // Read from row into newRow
                const auto columnStartOffset = row.ColumnStartOffset(columnToRead);
                const auto columnSize = row.ColumnSize(columnToRead);
                newRow.Append(row.Data(columnStartOffset), columnSize);
            }
            
            return newRow;
        }
        
    private:
        InstSerializedValuePtr _instructions;
    };
}
