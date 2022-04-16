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
        using RowIndexType = uint16_t;
        METAL_CONSTANT static constexpr int8_t RAW_DATA_NUM_ROWS_INDEX = 2;

        RawTable(METAL_DEVICE char* rawData) : _rawData(rawData) {}

        metaldb::types::SizeType GetSizeOfHeader() {
            return *((METAL_DEVICE metaldb::types::SizeType*) &_rawData[0 * sizeof(metaldb::types::SizeType)]);
        }

        metaldb::types::SizeType GetSizeOfData() {
            return *((METAL_DEVICE metaldb::types::SizeType*) &_rawData[1 * sizeof(metaldb::types::SizeType)]);
        }

        metaldb::types::SizeType GetNumRows() {
            return *((METAL_DEVICE metaldb::types::SizeType*) &_rawData[RAW_DATA_NUM_ROWS_INDEX * sizeof(metaldb::types::SizeType)]);
        }

        RowIndexType GetRowIndex(uint8_t index) {
            return *((METAL_DEVICE RowIndexType*) &_rawData[(RAW_DATA_NUM_ROWS_INDEX * sizeof(metaldb::types::SizeType)) + (1 + index) * sizeof(RowIndexType)]);
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
}
