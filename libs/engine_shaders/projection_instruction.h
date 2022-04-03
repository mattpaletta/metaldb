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

namespace metaldb {
    class ProjectionInstruction final {
    public:
        // Pointer points to beginning of Projection instruction.
        ProjectionInstruction(METAL_DEVICE int8_t* instructions) : _instructions(instructions) {}

        int8_t numColumns() const {
            return (int8_t) this->_instructions[0];
        }

        // This returns the column to read from the row.
        int8_t getColumnIndex(int8_t index) const {
            // +1 the size is the first item
            return (int8_t) this->_instructions[index + 1];
        }

        METAL_DEVICE int8_t* end() const {
            // Returns 1 past the end of the instruction
            const int8_t numColumnsOffset = 1;
            const int8_t columnsOffset = this->numColumns();
            const int8_t offset = numColumnsOffset + columnsOffset;
            return &this->_instructions[offset];
        }

        void GetRow(DbConstants METAL_THREAD & constants) {

        }

    private:
        METAL_DEVICE int8_t* _instructions;
    };
}
