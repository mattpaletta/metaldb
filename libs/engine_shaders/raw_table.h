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
        using SizeOfHeaderType = types::SizeType;
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;

        using SizeOfDataType = types::SizeType;
        METAL_CONSTANT static constexpr auto SizeOfDataOffset = sizeof(SizeOfHeaderType) + SizeOfHeaderOffset;

        using NumRowsType = types::SizeType;
        METAL_CONSTANT static constexpr auto NumRowsOffset = sizeof(SizeOfDataType) + SizeOfDataOffset;

        using RowIndexType = uint16_t;
        METAL_CONSTANT static constexpr auto RowIndexOffset = sizeof(NumRowsOffset) + NumRowsOffset;

        RawTable(METAL_DEVICE char* rawData) : _rawData(rawData) {}

        SizeOfHeaderType GetSizeOfHeader() const {
            return ReadBytesStartingAt<SizeOfHeaderType>(&_rawData[SizeOfHeaderOffset]);
        }

        SizeOfDataType GetSizeOfData() const {
            return ReadBytesStartingAt<SizeOfDataType>(&_rawData[SizeOfDataOffset]);
        }

        NumRowsType GetNumRows() const {
            return ReadBytesStartingAt<NumRowsType>(&_rawData[NumRowsOffset]);
        }

        RowIndexType GetRowIndex(metaldb::types::SizeType index) const {
            // Start row index is immediately after the getNumRows field.
            const auto indexToRead = RowIndexOffset + (index * sizeof(RowIndexType));
            return ReadBytesStartingAt<RowIndexType>(&_rawData[indexToRead]);
        }

        SizeOfDataType GetStartOfData() const {
            return this->GetSizeOfHeader();
        }

        METAL_DEVICE char* data(SizeOfDataType index = 0) {
            const auto dataStart = this->GetStartOfData();
            return &this->_rawData[dataStart + index];
        }

    private:
        METAL_DEVICE char* _rawData;
    };
}
