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
#include "raw_table.h"
#include "strings.h"

namespace metaldb {
    class ParseRowInstruction final {
    public:
        // Pointer points to beginning of ParseRow instruction.
        ParseRowInstruction(METAL_DEVICE int8_t* instructions) : _instructions(instructions) {}

        Method getMethod() const {
            return (Method) this->_instructions[0];
        }

        int8_t numColumns() const {
            return (int8_t) this->_instructions[1];
        }

        ColumnType getColumnType(int8_t index) const {
            // +2 because of the other two functions that are first in the serialization.
            return (ColumnType) this->_instructions[index + 2];
        }

        METAL_DEVICE int8_t* end() const {
            // Returns 1 past the end of the instruction
            const int8_t methodOffset = 1;
            const int8_t numColumnsOffset = 1;
            const int8_t columnsOffset = this->numColumns();
            const int8_t offset = methodOffset + numColumnsOffset + columnsOffset;
            return &this->_instructions[offset];
        }

        StringSection readCSVColumn(RawTable METAL_THREAD & rawTable, uint8_t row, uint8_t column) const {
            auto rowIndex = rawTable.GetRowIndex(row);

            // Get the column by scanning
            METAL_DEVICE char* startOfColumn = rawTable.data(rowIndex);
            for (uint8_t i = 0; i < column && startOfColumn; ++i) {
                startOfColumn = metal::strings::strchr(startOfColumn, ',');
            }

            if (!startOfColumn) {
                return StringSection(startOfColumn, 0);
            }

            METAL_DEVICE char* endOfColumn = metal::strings::strchr(startOfColumn, ',');

            return StringSection(startOfColumn, endOfColumn - startOfColumn);
        }

    private:
        METAL_DEVICE int8_t* _instructions;
    };
}
