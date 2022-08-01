#pragma once

#include "raw_table.h"
#include "constants.h"
#include "output_row.h"

namespace metaldb {
    class DbConstants final {
    public:
        constexpr static METAL_CONSTANT uint16_t MAX_NUM_ROWS = 1000;

        DbConstants(RawTable METAL_THREAD & rawTable_, OutputSerializedValue METAL_DEVICE * outputBuffer_, OutputRow::NumBytesType METAL_THREADGROUP * rowSizeScratch_) : rawTable(rawTable_), outputBuffer(outputBuffer_), rowSizeScratch(rowSizeScratch_) {}

        // Metal complains if you try and pass a reference here, so we use a pointer instead.
        RawTable METAL_THREAD & rawTable;
        OutputSerializedValue METAL_DEVICE * outputBuffer;

        uint thread_position_in_grid = 0;
        uint threadgroup_position_in_grid = 0;
        uint thread_position_in_threadgroup = 0;
        ushort thread_execution_width = 0;

        OutputRow::NumBytesType METAL_THREADGROUP * rowSizeScratch;
    };
}
