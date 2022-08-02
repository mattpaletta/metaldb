#pragma once

#include "constants.h"

namespace metaldb {
    class RawTable {
    public:
        using value_type = METAL_DEVICE char*;

        /**
         * The type to store the size of the header.
         */
        using SizeOfHeaderType = types::SizeType;

        /**
         * The starting offset for the size of the header in the @b RawTable .
         */
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;

        /**
         * The type to store the size of the data
         */
        using SizeOfDataType = types::SizeType;

        /**
         * The starting offset for the size of the data.  This immediately succeeds the size of the header.
         */
        METAL_CONSTANT static constexpr auto SizeOfDataOffset = sizeof(SizeOfHeaderType) + SizeOfHeaderOffset;

        /**
         * The type to store the number of rows in the RawTable
         */
        using NumRowsType = types::SizeType;

        /**
         * The starting offset for the number of rows in the RawTable.  This immediately succeeds the size of the data.
         */
        METAL_CONSTANT static constexpr auto NumRowsOffset = sizeof(SizeOfDataType) + SizeOfDataOffset;

        /**
         * The type to store the starting offset for each row within the data section.
         */
        using RowIndexType = uint32_t;

        /**
         * The starting offset for the offsets of each row.  This immediately succeeds the number of rows.
         */
        METAL_CONSTANT static constexpr auto RowIndexOffset = sizeof(NumRowsOffset) + NumRowsOffset;

        RawTable(value_type rawData) : _rawData(rawData) {}

        /**
         * Returns the size of the header in bytes.
         */
        CPP_PURE_FUNC SizeOfHeaderType GetSizeOfHeader() const {
            return ReadBytesStartingAt<SizeOfHeaderType>(&_rawData[SizeOfHeaderOffset]);
        }

        /**
         * Returns the size of the data in bytes.
         */
        CPP_PURE_FUNC SizeOfDataType GetSizeOfData() const {
            return ReadBytesStartingAt<SizeOfDataType>(&_rawData[SizeOfDataOffset]);
        }

        /**
         * Returns the number of rows in the object.
         */
        CPP_PURE_FUNC NumRowsType GetNumRows() const {
            return ReadBytesStartingAt<NumRowsType>(&_rawData[NumRowsOffset]);
        }

        /**
         * Returns the starting data offset for a particular row number.
         * For example, if every row is exactly 10 bytes, the row index will be: `row * 10`
         */
        CPP_PURE_FUNC RowIndexType GetRowIndex(NumRowsType index) const {
            // Start row index is immediately after the getNumRows field.
            const auto indexToRead = RowIndexOffset + (index * sizeof(RowIndexType));
            return ReadBytesStartingAt<RowIndexType>(&_rawData[indexToRead]);
        }

        /**
         * Returns the index for the start of the data section.
         * The data section immediately succeeds the header, so this is equivalent to getting the size of the header.
         */
        CPP_PURE_FUNC SizeOfDataType GetStartOfData() const {
            return this->GetSizeOfHeader();
        }

        /**
         * Retreives a byte pointer from the data field.
         * The size of the row at that index can be calculated by getting the row index of the current row and of the next row.
         */
        CPP_PURE_FUNC value_type Data(SizeOfDataType index = 0) {
            const auto dataStart = this->GetStartOfData();
            return &this->_rawData[dataStart + index];
        }

    private:
        value_type _rawData;
    };
}
