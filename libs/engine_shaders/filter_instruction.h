//
//  filter_instruction.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-04-18.
//

#pragma once

#include "constants.h"
#include "instruction_type.h"
#include "stack.h"

namespace metaldb {
    class FilterInstruction final {
        enum Operation : instruction_serialized_value_type {
            READ_FLOAT,
            READ_INT,
            READ_STRING,
            CAST_FLOAT_INT,
            CAST_INT_FLOAT,
            GT_FLOAT,
            GT_INT,
            LT_FLOAT,
            LT_INT,
            GTE_FLOAT,
            GTE_INT,
            EQ_FLOAT,
            EQ_INT,
            NE_FLOAT,
            NE_INT
        };

        // Pointer points to beginning of Projection instruction.
        FilterInstruction(METAL_DEVICE int8_t* instructions) : _instructions(instructions) {}

        uint16_t NumOperations() const {
            return *((uint16_t METAL_DEVICE *) &this->_instructions[0]);
        }

        instruction_serialized_value_type GetOperation(size_t i) const {
            const auto index = sizeof(this->NumOperations()) + (i * sizeof(instruction_serialized_value_type));
            return *((instruction_serialized_value_type METAL_DEVICE *) &this->_instructions[index]);
        }

        METAL_DEVICE int8_t* end() const {
            // Returns 1 past the end of the instruction
            const auto index = sizeof(this->NumOperations()) +
                sizeof(this->GetOperation(0)) * this->NumOperations();
            return &this->_instructions[index];
        }

        TempRow GetRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) const {
            if (this->ShouldIncludeRow(row, constants)) {
                return row;
            } else {
                // If excluded, return the empty row.
                return TempRow();
            }
        }

    private:
        METAL_DEVICE int8_t* _instructions;

        bool ShouldIncludeRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) const {
            // TODO: Using a stack with different sized elements.
        }
    };
}
