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
    static InstructionType decodeType(InstSerializedValuePtr instruction) {
        return (InstructionType) *instruction;
    }

    static void runInstructions(InstSerializedValuePtr instructions, size_t numInstructions, DbConstants METAL_THREAD & constants) {
        // Combines decoding instructions and running them.

        TempRow row;
        InstSerializedValuePtr currentInstruction = instructions;

        for (size_t i = 0; i < numInstructions; ++i) {
            switch (decodeType(currentInstruction)) {
            case metaldb::PARSEROW: {
                auto parseRowExpression = ParseRowInstruction(&currentInstruction[1]);
                row = parseRowExpression.GetRow(constants);

                currentInstruction = parseRowExpression.end();
                break;
            }
            case metaldb::PROJECTION: {
                auto projectionInstruction = ProjectionInstruction(&currentInstruction[1]);
                row = projectionInstruction.GetRow(row, constants);

                currentInstruction = projectionInstruction.end();
                break;
            }
            case metaldb::FILTER: {
                auto filterInstruction = FilterInstruction(&currentInstruction[1]);
                row = filterInstruction.GetRow(row, constants);

                currentInstruction = filterInstruction.end();
                break;
            }
            case metaldb::OUTPUT: {
                auto outputProjection = OutputInstruction(&currentInstruction[1]);
                outputProjection.WriteRow(row, constants);

                //            currentInstruction = outputProjection.end();
                break;
            }
            }

#ifdef __METAL__
            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
#endif
        }
    }
}
