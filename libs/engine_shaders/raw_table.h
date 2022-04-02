//
//  raw_table.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "constants.h"

namespace metaldb {
    class RawTable {
    public:
        RawTable(METAL_DEVICE char* rawData) : _rawData(rawData) {}

        METAL_CONSTANT static constexpr int8_t RAW_DATA_NUM_ROWS_INDEX = 2;

        int8_t GetSizeOfHeader() {
            return (int8_t) _rawData[0];
        }

        int8_t GetSizeOfData() {
            return (int8_t) _rawData[1];
        }

        int8_t GetNumRows() {
            return (int8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX];
        }

        int8_t GetRowIndex(int8_t index) {
            return (int8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX + 1 + index];
        }

        int8_t GetStartOfData() {
            return GetSizeOfHeader() + 1;
        }

        METAL_DEVICE char* data(uint8_t index = 0) {
            return &this->_rawData[index];
        }

    private:
        METAL_DEVICE char* _rawData;
    };
};
