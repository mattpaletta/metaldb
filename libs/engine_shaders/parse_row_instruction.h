//
//  parse_row_instruction.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "constants.h"
#include "column_type.h"
#include "method.h"
#include "string_section.h"
#include "temp_row.h"
#include "raw_table.h"
#include "db_constants.h"
#include "strings.h"

namespace metaldb {
    class ParseRowInstruction {
    public:
        using MethodType = Method;
        METAL_CONSTANT static constexpr auto MethodOffset = 0;

        using SkipHeaderType = bool;
        METAL_CONSTANT static constexpr auto SkipHeaderOffset = sizeof(MethodType) + MethodOffset;

        using NumColumnsType = OutputRow::NumColumnsType;
        METAL_CONSTANT static constexpr auto NumColumnsOffset = sizeof(SkipHeaderType) + SkipHeaderOffset;

        METAL_CONSTANT static constexpr auto ColumnTypeOffset = sizeof(NumColumnsType) + NumColumnsOffset;

        using ColumnSizeType = TempRow::ColumnSizeType;
        using RowNumType = uint16_t;

        // Pointer points to beginning of ParseRow instruction.
#ifndef __METAL__
        ParseRowInstruction() : _instructions(nullptr) {}
#endif
        ParseRowInstruction(InstSerializedValuePtr instructions) : _instructions(instructions) {}

        MethodType getMethod() const {
            return ReadBytesStartingAt<MethodType>(&this->_instructions[MethodOffset]);
        }

        SkipHeaderType skipHeader() const {
            return ReadBytesStartingAt<SkipHeaderType>(&this->_instructions[SkipHeaderOffset]);
        }

        NumColumnsType numColumns() const {
            return ReadBytesStartingAt<NumColumnsType>(&this->_instructions[NumColumnsOffset]);
        }

        ColumnType getColumnType(NumColumnsType index) const {
            return ReadBytesStartingAt<ColumnType>(&this->_instructions[(index * sizeof(ColumnType)) + ColumnTypeOffset]);
        }

        InstSerializedValuePtr end() const {
            // Returns 1 past the end of the instruction
            const auto numColumns = this->numColumns();
            const auto offset = ColumnTypeOffset + (numColumns * sizeof(ColumnType));
            return &this->_instructions[offset];
        }

        StringSection readCSVColumn(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const {
            auto rowIndex = rawTable.GetRowIndex(this->skipHeader() ? row+1 : row);

            // Get the column by scanning
            METAL_DEVICE char* startOfColumn = rawTable.data(rowIndex);

            for (NumColumnsType i = 0; i < column && startOfColumn; ++i) {
                startOfColumn = metal::strings::strchr(startOfColumn, ',');
                if (startOfColumn) {
                    startOfColumn++;
                }
            }

            if (!startOfColumn) {
                return StringSection(startOfColumn, 0);
            }

            ColumnSizeType length = 0;
            if (row == rawTable.GetNumRows() - 1 && column == this->numColumns() - 1) {
                length = ((ColumnSizeType) (rawTable.data(rawTable.GetSizeOfData() - 1) - startOfColumn)) + 1;
            } else if (row < rawTable.GetNumRows()) {
                // Only safe to calculate the end of the column if not the last row.
                // Otherwise could read past the end of the buffer.
                METAL_DEVICE char* endOfColumn = metal::strings::strchr(startOfColumn, ',');

                // Unless it's the last row, read until the start of the next row.
                auto startOfNextRowInd = rawTable.GetRowIndex(this->skipHeader() ? row+2 : row+1);
                METAL_DEVICE char* startOfNextRow = rawTable.data(startOfNextRowInd);
                auto lengthOfThisColumn = endOfColumn - startOfColumn;
                auto lengthToNextRow = startOfNextRow - startOfColumn;
                length = lengthOfThisColumn > lengthToNextRow ? lengthToNextRow : lengthOfThisColumn;
            }

            // TODO: Must be matching quotes.
            if ((*startOfColumn == '"' || *startOfColumn == '\'') && (*(startOfColumn+length-1) == '"' || (*(startOfColumn+length-1) == '"'))) {
                // Starts and ends with a quote, strip them.
                return StringSection(startOfColumn + 1, length - 2);
            }

            return StringSection(startOfColumn, length);
        }

        ColumnSizeType readCSVColumnLength(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const {
            // Do it this way for now to keep the code the same.
            return this->readCSVColumn(rawTable, row, column).size();
        }

        TempRow GetRow(DbConstants METAL_THREAD & constants) {
            TempRow::TempRowBuilder builder;
            auto numCols = this->numColumns();
            {
                builder.numColumns = numCols;

                // Read column types
                for (auto i = 0; i < numCols; ++i) {
                    // Set all column types
                    auto columnType = this->getColumnType(i);
                    builder.columnTypes[i] = columnType;

                    // Set all column sizes, and they might get pruned
                    if (columnType == String) {
                        builder.columnSizes[i] = this->readCSVColumnLength(constants.rawTable, constants.thread_position_in_grid, i);
                    } else {
                        builder.columnSizes[i] = 0;
                    }
                }
            }

            // Populate the row.
            TempRow row = builder;
            for (auto i = 0; i < numCols; ++i) {
                // Write the columns into the buffer
                auto stringSection = this->readCSVColumn(constants.rawTable, constants.thread_position_in_grid, i);

                switch (this->getColumnType(i)) {
                case String: {
                    // Copy the string in directly.
                    row.append(stringSection.c_str(), stringSection.size());
                    break;
                }
                case Integer: {
                    // Cast it to an integer
                    types::IntegerType result = metal::strings::stoi(stringSection.c_str(), stringSection.size());
                    row.append(result);
                    break;
                }
                case Float: {
                    // Cast it to a float.
                    types::FloatType result = metal::strings::stof(stringSection.c_str(), stringSection.size());
                    row.append(result);
                    break;
                }
                case Unknown:
                    break;
                }
            }

            return row;
        }

    private:
        InstSerializedValuePtr _instructions;
    };
}
