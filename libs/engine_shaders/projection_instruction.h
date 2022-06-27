//
//  projection_instruction.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "constants.h"
#include "column_type.h"
#include "raw_table.h"
#include "db_constants.h"
#include "temp_row.h"

namespace metaldb {
    class ProjectionInstruction {
    public:
        using NumColumnsType = OutputRow::NumColumnsType;
        METAL_CONSTANT static constexpr auto NumColumnsOffset = 0;

        using ColumnIndexType = NumColumnsType;
        METAL_CONSTANT static constexpr auto ColumnIndexOffset = sizeof(NumColumnsType) + NumColumnsOffset;

#ifndef __METAL__
        ProjectionInstruction() : _instructions(nullptr) {}
#endif
        // Pointer points to beginning of Projection instruction.
        ProjectionInstruction(InstSerializedValuePtr instructions) : _instructions(instructions) {}

        NumColumnsType numColumns() const {
            return ReadBytesStartingAt<NumColumnsType>(&this->_instructions[NumColumnsOffset]);
        }

        // This returns the column to read from the row.
        ColumnIndexType getColumnIndex(NumColumnsType index) const {
            return ReadBytesStartingAt<ColumnIndexType>(&this->_instructions[(index * sizeof(ColumnIndexType)) + ColumnIndexOffset]);
        }

        InstSerializedValuePtr end() const {
            // Returns 1 past the end of the instruction
            const auto numColumns = this->numColumns();
            const uint8_t offset = ColumnIndexOffset + (numColumns * sizeof(ColumnIndexType));
            return &this->_instructions[offset];
        }

        TempRow GetRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) {
            // Do the projection
            TempRow::TempRowBuilder builder;
            auto numCols = this->numColumns();
            {
                builder.numColumns = numCols;

                // Read column types
                for (auto i = 0; i < numCols; ++i) {
                    auto columnToRead = this->getColumnIndex(i);

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
                const auto columnToRead = this->getColumnIndex(i);

                // Read from row into newRow
                const auto columnStartOffset = row.ColumnStartOffset(columnToRead);
                const auto columnSize = row.ColumnSize(columnToRead);
                newRow.append(row.data(columnStartOffset), columnSize);
            }

            return newRow;
        }

    private:
        InstSerializedValuePtr _instructions;
    };
}
