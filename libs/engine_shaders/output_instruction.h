//
//  output_instruction.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-31.
//

#pragma once

#include "constants.h"
#include "db_constants.h"
#include "strings.h"

namespace metaldb {
    class OutputInstruction final {
    public:
        OutputInstruction(METAL_DEVICE int8_t* instructions) : _instructions(instructions) {}

        METAL_DEVICE int8_t* end() const {
            // Returns 1 past the end of the instruction
            const int8_t offset = 1;
            return &this->_instructions[offset];
        }

        void WriteRow(DbConstants METAL_THREAD & constants) {

        }

    private:
        METAL_DEVICE int8_t* _instructions;
    };
}
