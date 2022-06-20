#include "strings.h"
#include "constants.h"
#include "engine.h"

kernel void runQueryKernel(device char* rawData [[ buffer(0) ]], metaldb::InstSerializedValuePtr instructions [[ buffer(1) ]], device int8_t* outputBuffer [[ buffer(2) ]], uint id [[ thread_position_in_grid ]], uint group_id [[threadgroup_position_in_grid]], uint local_id [[thread_position_in_threadgroup]], ushort simd_width [[ thread_execution_width ]]) {
    // declare this here as this can only be declared in a 'kernel'.
    threadgroup metaldb::OutputRow::NumBytesType rowSizeScratch[metaldb::DbConstants::MAX_NUM_ROWS];

    metaldb::RawTable rawTable(rawData);

    // Decode instructions + dispatch
    const auto numInstructions = instructions[0];
    metaldb::DbConstants constants{rawTable, outputBuffer, rowSizeScratch};

    constants.thread_position_in_grid = id;
    constants.threadgroup_position_in_grid = group_id;
    constants.thread_position_in_threadgroup = local_id;
    constants.thread_execution_width = simd_width;

    metaldb::runInstructions(&instructions[1], numInstructions, constants);
}
