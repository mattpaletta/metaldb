//
//  db_constants.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-04-02.
//

#pragma once

#include "raw_table.h"
#include "constants.h"

namespace metaldb {
    class DbConstants final {
    public:
        constexpr static METAL_CONSTANT uint16_t MAX_NUM_ROWS = 1000;

        DbConstants(RawTable METAL_THREAD & rawTable_, METAL_DEVICE int8_t* outputBuffer_, uint32_t METAL_THREADGROUP * rowSizeScratch_) : rawTable(rawTable_), outputBuffer(outputBuffer_), rowSizeScratch(rowSizeScratch_) {}

        // Metal complains if you try and pass a reference here, so we use a pointer instead.
        RawTable METAL_THREAD & rawTable;
        METAL_DEVICE int8_t* outputBuffer;

        uint thread_position_in_grid = 0;
        uint threadgroup_position_in_grid = 0;
        uint thread_position_in_threadgroup = 0;
        ushort thread_execution_width = 0;

        METAL_THREADGROUP uint32_t* rowSizeScratch;

#ifndef __METAL__
        static size_t currentOutputBufferSize;
#endif
    };

#ifndef __METAL__
    inline size_t DbConstants::currentOutputBufferSize = 0;
#endif
}
