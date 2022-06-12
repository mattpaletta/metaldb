//
//  output_row.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-06-02.
//

#pragma once

#include "string_section.h"

namespace metaldb {
    class OutputRow {
    public:
        using SizeOfHeaderType = uint16_t;
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;

        using NumBytesType = uint32_t;
        METAL_CONSTANT static constexpr auto NumBytesOffset = sizeof(SizeOfHeaderType) + SizeOfHeaderOffset;

        using NumColumnsType = uint8_t;
        METAL_CONSTANT static constexpr auto NumColumnsOffset = sizeof(NumBytesType) + NumBytesOffset;

        METAL_CONSTANT static constexpr auto ColumnTypeOffset = sizeof(NumColumnsType) + NumColumnsOffset;

        using ColumnSizeType = StringSection::SizeType;
    };
}
