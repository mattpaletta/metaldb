#pragma once

#include "constants.h"

namespace metaldb {
    class RawTable {
    public:
        using value_type = METAL_DEVICE char*;

        using SizeOfHeaderType = types::SizeType;
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;

        using SizeOfDataType = types::SizeType;
        METAL_CONSTANT static constexpr auto SizeOfDataOffset = sizeof(SizeOfHeaderType) + SizeOfHeaderOffset;

        using NumRowsType = types::SizeType;
        METAL_CONSTANT static constexpr auto NumRowsOffset = sizeof(SizeOfDataType) + SizeOfDataOffset;

        using RowIndexType = uint32_t;
        METAL_CONSTANT static constexpr auto RowIndexOffset = sizeof(NumRowsOffset) + NumRowsOffset;

        RawTable(value_type rawData) : _rawData(rawData) {}

        CPP_PURE_FUNC SizeOfHeaderType GetSizeOfHeader() const {
            return ReadBytesStartingAt<SizeOfHeaderType>(&_rawData[SizeOfHeaderOffset]);
        }

        CPP_PURE_FUNC SizeOfDataType GetSizeOfData() const {
            return ReadBytesStartingAt<SizeOfDataType>(&_rawData[SizeOfDataOffset]);
        }

        CPP_PURE_FUNC NumRowsType GetNumRows() const {
            return ReadBytesStartingAt<NumRowsType>(&_rawData[NumRowsOffset]);
        }

        CPP_PURE_FUNC RowIndexType GetRowIndex(metaldb::types::SizeType index) const {
            // Start row index is immediately after the getNumRows field.
            const auto indexToRead = RowIndexOffset + (index * sizeof(RowIndexType));
            return ReadBytesStartingAt<RowIndexType>(&_rawData[indexToRead]);
        }

        CPP_PURE_FUNC SizeOfDataType GetStartOfData() const {
            return this->GetSizeOfHeader();
        }

        CPP_PURE_FUNC value_type data(SizeOfDataType index = 0) {
            const auto dataStart = this->GetStartOfData();
            return &this->_rawData[dataStart + index];
        }

    private:
        value_type _rawData;
    };
}
