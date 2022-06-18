//
//  output_row.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-06-02.
//

#pragma once

#include "constants.h"
#include "string_section.h"

namespace metaldb {
    namespace details {
        template<typename T>
        static constexpr size_t DetermineAlignment(size_t baseOffset) {
            constexpr auto tAlign = alignof(T);
            return baseOffset + (baseOffset % tAlign);
        }
    }

    class OutputRow {
    public:

        using SizeOfHeaderType = uint16_t;
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;

        /**
         * Num bytes include the size of the header.
         */
        using NumBytesType = uint32_t;
        METAL_CONSTANT static constexpr auto NumBytesOffset = details::DetermineAlignment<NumBytesType>(sizeof(SizeOfHeaderType) + SizeOfHeaderOffset);

        using NumColumnsType = uint8_t;
        METAL_CONSTANT static constexpr auto NumColumnsOffset = details::DetermineAlignment<NumColumnsType>(sizeof(NumBytesType) + NumBytesOffset);

        METAL_CONSTANT static constexpr auto ColumnTypeOffset = details::DetermineAlignment<ColumnType>(sizeof(NumColumnsType) + NumColumnsOffset);

        using ColumnSizeType = StringSection::SizeType;

        // Only used by parsers.
        using NumRowsType = uint32_t;
    };
}
