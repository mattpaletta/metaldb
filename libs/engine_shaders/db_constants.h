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
        DbConstants(metaldb::RawTable METAL_THREAD & rawTable_, METAL_DEVICE int8_t* outputBuffer_, uint8_t thread_position_in_grid_) : rawTable(rawTable_), outputBuffer(outputBuffer_), thread_position_in_grid(thread_position_in_grid_) {}

        metaldb::RawTable METAL_THREAD & rawTable;
        METAL_DEVICE int8_t* outputBuffer;
        uint8_t thread_position_in_grid;
        
#ifndef __METAL__
        static size_t currentOutputBufferSize;
#endif
    };

#ifndef __METAL__
    inline size_t DbConstants::currentOutputBufferSize = 0;
#endif
}
