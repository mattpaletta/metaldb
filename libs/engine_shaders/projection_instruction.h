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
    class ProjectionInstruction final {
    public:
        // Pointer points to beginning of Projection instruction.
        ProjectionInstruction(METAL_DEVICE int8_t* instructions) : _instructions(instructions) {}

        uint8_t numColumns() const {
            return this->_instructions[0];
        }

        // This returns the column to read from the row.
        uint8_t getColumnIndex(uint8_t index) const {
            // +1 the size is the first item
            return this->_instructions[index + 1];
        }

        METAL_DEVICE int8_t* end() const {
            // Returns 1 past the end of the instruction
            const uint8_t numColumnsOffset = 1;
            const uint8_t columnsOffset = this->numColumns();
            const uint8_t offset = numColumnsOffset + columnsOffset;
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
                    if (columnType == String) {
                        builder.columnSizes[i] = row.ColumnSize(columnToRead);
                    } else {
                        builder.columnSizes[i] = 0;
                    }
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
        METAL_DEVICE int8_t* _instructions;
    };
}
