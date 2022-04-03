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

        uint8_t GetSizeOfHeader() {
            return (uint8_t) _rawData[0];
        }

        uint8_t GetSizeOfData() {
            return (uint8_t) _rawData[1];
        }

        uint8_t GetNumRows() {
            return (uint8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX];
        }

        uint8_t GetRowIndex(uint8_t index) {
            return (uint8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX + 1 + index];
        }

        uint8_t GetStartOfData() {
            return GetSizeOfHeader() + 1;
        }

        METAL_DEVICE char* data(uint8_t index = 0) {
            const auto headerSize = this->GetSizeOfHeader();
            return &this->_rawData[headerSize + index];
        }

    private:
        METAL_DEVICE char* _rawData;
    };
};
