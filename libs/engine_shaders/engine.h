#pragma once

#include "constants.h"
#include "raw_table.h"
#include "instruction_type.h"
#include "parse_row_instruction.h"
#include "projection_instruction.h"
#include "filter_instruction.h"
#include "output_instruction.h"
#include "method.h"
#include "temp_row.h"
#include "db_constants.h"

namespace metaldb {
    /**
     * Dereferences a pointer as an @b InstructionType .
     */
    static InstructionType DecodeType(InstSerializedValuePtr instruction) CPP_NOEXCEPT {
        return (InstructionType) *instruction;
    }

    /**
     * Executes a sequence of metaldb instructions.
     * @param instructions A pointer to the beginning of the first instruction.  The first value should be an encoded version
     *                    of @b InstructionType , followed by the encoded value of that @b InstructionType .
     * @param numInstructions The number of instructions to execute that are stored in @b instructions
     * @param constants The struct of constants used by the instructions.
     *
     * In metal, this will wait for all threads within the threadgroup before continuing onto the next instruction.
     */
    static void RunInstructions(InstSerializedValuePtr instructions, size_t numInstructions, DbConstants METAL_THREAD & constants) CPP_NOEXCEPT {
        // Combines decoding instructions and running them.

        TempRow row;
        InstSerializedValuePtr currentInstruction = instructions;

        for (size_t i = 0; i < numInstructions; ++i) {
            switch (DecodeType(currentInstruction)) {
            case metaldb::PARSEROW: {
                auto parseRowExpression = ParseRowInstruction(&currentInstruction[1]);
                row = parseRowExpression.GetRow(constants);

                currentInstruction = parseRowExpression.End();
                break;
            }
            case metaldb::PROJECTION: {
                auto projectionInstruction = ProjectionInstruction(&currentInstruction[1]);
                row = projectionInstruction.GetRow(row, constants);

                currentInstruction = projectionInstruction.End();
                break;
            }
            case metaldb::FILTER: {
                auto filterInstruction = FilterInstruction(&currentInstruction[1]);
                row = filterInstruction.GetRow(row, constants);

                currentInstruction = filterInstruction.End();
                break;
            }
            case metaldb::OUTPUT: {
                auto outputInstruction = OutputInstruction(&currentInstruction[1]);
                outputInstruction.WriteRow(row, constants);
                currentInstruction = outputInstruction.End();
                break;
            }
            }

#ifdef __METAL__
            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
#endif
        }
    }
}
