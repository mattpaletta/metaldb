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
        RawTable(METAL_DEVICE char* rawData) : _rawData(rawData) {}

        metaldb::types::SizeType GetSizeOfHeader() const {
            return *((METAL_DEVICE metaldb::types::SizeType*) &_rawData[0]);
        }

        metaldb::types::SizeType GetSizeOfData() const {
            constexpr auto index = sizeof(this->GetSizeOfHeader());
            return *((METAL_DEVICE metaldb::types::SizeType*) &_rawData[index]);
        }

        metaldb::types::SizeType GetNumRows() const {
            constexpr auto index = sizeof(this->GetSizeOfHeader()) + sizeof(this->GetSizeOfData());
            return *((METAL_DEVICE metaldb::types::SizeType*) &_rawData[index]);
        }

        RowIndexType GetRowIndex(metaldb::types::SizeType index) const {
            // Start row index is immediately after the getNumRows field.
            constexpr auto startRowIndex = sizeof(this->GetSizeOfHeader()) + sizeof(this->GetSizeOfData()) + sizeof(this->GetNumRows());
            const auto indexToRead = startRowIndex + (index * sizeof(RowIndexType));
            return *((METAL_DEVICE RowIndexType*) &_rawData[indexToRead]);
        }

        uint8_t GetStartOfData() const {
            return this->GetSizeOfHeader() + 1;
        }

        METAL_DEVICE char* data(metaldb::types::SizeType index = 0) {
            const auto headerSize = this->GetSizeOfHeader();
            return &this->_rawData[headerSize + index];
        }

    private:
        METAL_DEVICE char* _rawData;
    };
}
