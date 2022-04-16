#include "strings.h"
#include "constants.h"
#include "engine.h"

static metaldb::InstructionType decodeType(METAL_DEVICE int8_t* instruction) {
    return (metaldb::InstructionType) *instruction;
}

void runInstructions(METAL_DEVICE int8_t* instructions, size_t numInstructions, metaldb::DbConstants METAL_THREAD & constants) {
    // Combines decoding instructions and running them.

    metaldb::TempRow row;
    METAL_DEVICE int8_t* currentInstruction = instructions;

    for (size_t i = 0; i < numInstructions; ++i) {
        switch (decodeType(currentInstruction)) {
        case metaldb::PARSEROW: {
            auto parseRowExpression = metaldb::ParseRowInstruction(&currentInstruction[1]);
            row = parseRowExpression.GetRow(constants);

            currentInstruction = parseRowExpression.end();
            break;
        }
        case metaldb::PROJECTION: {
            auto projectionInstruction = metaldb::ProjectionInstruction(&currentInstruction[1]);
            row = projectionInstruction.GetRow(row, constants);

            currentInstruction = projectionInstruction.end();
            break;
        }
        case metaldb::OUTPUT: {
            auto outputProjection = metaldb::OutputInstruction(&currentInstruction[1]);
            outputProjection.WriteRow(row, constants);

//            currentInstruction = outputProjection.end();
            break;
        }
        }

        threadgroup_barrier(metal::mem_flags::mem_threadgroup);
    }
}

kernel void runQueryKernelBackup(device char* rawData [[ buffer(0) ]], device int8_t* instructions [[ buffer(1) ]], device int8_t* outputBuffer [[ buffer(2) ]], uint id [[ thread_position_in_grid ]], uint group_id [[threadgroup_position_in_grid]], uint local_id [[thread_position_in_threadgroup]]) {
    // declare this here as this can only be declared in a 'kernel'.
    threadgroup uint32_t rowSizeScratch[metaldb::DbConstants::MAX_NUM_ROWS];

    metaldb::RawTable rawTable(rawData);

    // Decode instructions + dispatch
    const auto numInstructions = instructions[0];
    metaldb::DbConstants constants{rawTable, outputBuffer, id, group_id, local_id, rowSizeScratch};

    runInstructions(&instructions[1], numInstructions, constants);
}
