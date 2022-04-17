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

        DbConstants(RawTable METAL_THREAD & rawTable_, METAL_DEVICE int8_t* outputBuffer_, uint thread_position_in_grid_, uint threadgroup_position_in_grid_, uint thread_position_in_threadgroup_, ushort thread_execution_width_, uint32_t METAL_THREADGROUP * rowSizeScratch_) : rawTable(rawTable_), outputBuffer(outputBuffer_), thread_position_in_grid(thread_position_in_grid_), threadgroup_position_in_grid(threadgroup_position_in_grid_), thread_position_in_threadgroup(thread_position_in_threadgroup_), thread_execution_width(thread_execution_width_), rowSizeScratch(rowSizeScratch_) {}

        // Metal complains if you try and pass a reference here, so we use a pointer instead.
        RawTable METAL_THREAD & rawTable;
        METAL_DEVICE int8_t* outputBuffer;
        uint thread_position_in_grid;
        uint threadgroup_position_in_grid;
        uint thread_position_in_threadgroup;
        ushort thread_execution_width;


        METAL_THREADGROUP uint32_t* rowSizeScratch;

#ifndef __METAL__
        static size_t currentOutputBufferSize;
#endif
    };

#ifndef __METAL__
    inline size_t DbConstants::currentOutputBufferSize = 0;
#endif
}
