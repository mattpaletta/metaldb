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
    class ParseRowInstruction final {
    public:
        // Pointer points to beginning of ParseRow instruction.
        ParseRowInstruction(METAL_DEVICE int8_t* instructions) : _instructions(instructions) {}

        Method getMethod() const {
            return (Method) this->_instructions[0];
        }

        bool skipHeader() const {
            return (bool) this->_instructions[1];
        }

        int8_t numColumns() const {
            return (int8_t) this->_instructions[2];
        }

        ColumnType getColumnType(int8_t index) const {
            // +3 because of the other two functions that are first in the serialization.
            return (ColumnType) this->_instructions[index + 3];
        }

        METAL_DEVICE int8_t* end() const {
            // Returns 1 past the end of the instruction
            const int8_t methodOffset = 1;
            const int8_t numColumnsOffset = 1;
            const int8_t columnsOffset = this->numColumns();
            const int8_t skipRow = 1;
            const int8_t offset = methodOffset + numColumnsOffset + columnsOffset + skipRow;
            return &this->_instructions[offset];
        }

        StringSection readCSVColumn(RawTable METAL_THREAD & rawTable, uint8_t row, uint8_t column) const {
            auto rowIndex = rawTable.GetRowIndex(this->skipHeader() ? row+1 : row);

            // Get the column by scanning
            METAL_DEVICE char* startOfColumn = rawTable.data(rowIndex);
            for (uint8_t i = 0; i < column && startOfColumn; ++i) {
                startOfColumn = metal::strings::strchr(startOfColumn, ',') + 1;
            }

            if (!startOfColumn) {
                return StringSection(startOfColumn, 0);
            }

            METAL_DEVICE char* endOfColumn = metal::strings::strchr(startOfColumn, ',');

            size_t length = endOfColumn - startOfColumn;
            if (row < rawTable.GetNumRows()) {
                // Unless it's the last row, read until the start of the next row.
                auto startOfNextRowInd = rawTable.GetRowIndex(this->skipHeader() ? row+2 : row+1);
                METAL_DEVICE char* startOfNextRow = rawTable.data(startOfNextRowInd);
                auto lengthOfThisColumn = endOfColumn - startOfColumn;
                auto lengthToNextRow = startOfNextRow - startOfColumn;
                length = lengthOfThisColumn > lengthToNextRow ? lengthToNextRow : lengthOfThisColumn;
            }

            if ((*startOfColumn == '"' || *startOfColumn == '\'') && (*(startOfColumn+length) == '"' || (*(startOfColumn+length) == '"'))) {
                // Starts and ends with a quote, strip them.
                return StringSection(startOfColumn + 1, length - 2);
            }

            return StringSection(startOfColumn, length);
        }

        int8_t readCSVColumnLength(RawTable METAL_THREAD & rawTable, uint8_t row, uint8_t column) const {
            // Do it this was for now to keep the code the same.
            return this->readCSVColumn(rawTable, row, column).size();
        }

        TempRow GetRow(DbConstants METAL_THREAD & constants) {
            // Question: do I have to read all columns first to get their sizes?

            TempRow::TempRowBuilder builder;
            {
                builder.numColumns = this->numColumns();

                // Read column types
                for (int8_t i = 0; i < this->numColumns(); ++i) {
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
            for (int8_t i = 0; i < this->numColumns(); ++i) {
                // Write the columns into the buffer
                auto stringSection = this->readCSVColumn(constants.rawTable, constants.thread_position_in_grid, i);

                switch (this->getColumnType(i)) {
                case String: {
                    // Copy the string in directly.
                    row.append(stringSection.str(), stringSection.size());
                    break;
                }
                case Integer: {
                    // Cast it to an integer
                    types::IntegerType result = metal::strings::stoi(stringSection.str(), stringSection.size());
                    row.append((char METAL_THREAD *) &result, sizeof(types::IntegerType));
                    break;
                }
                case Float: {
                    // Cast it to a float.
                    types::FloatType result = metal::strings::stof(stringSection.str(), stringSection.size());
                    row.append((char METAL_THREAD *) &result, sizeof(types::FloatType));
                    break;
                }
                case Unknown:
                    break;
                }
            }

            return row;
        }

    private:
        METAL_DEVICE int8_t* _instructions;
    };
}
