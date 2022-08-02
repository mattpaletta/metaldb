#pragma once

#include "constants.h"
#include "string_section.h"

namespace metaldb {
    namespace details {
        /**
         * Returns the next slot past @b baseOffset that will make type @b T stored in an aligned memory address.
         */
        template<typename T>
        static constexpr size_t DetermineAlignment(size_t baseOffset) {
            constexpr auto tAlign = alignof(T);
            return baseOffset + (baseOffset % tAlign);
        }
    }

    class OutputRow {
    public:
        /**
         * The type to store the size of the header in the OutputRow.
         */
        using SizeOfHeaderType = uint16_t;

        /**
         * The starting index for the size of the header in the OutputRow.
         */
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;

        /**
         * The type to store the number of bytes in the OutputRow.  The value includes the size of the header.
         * The size of the data portion only can be computed with `NumBytes - SizeOfHeader`.
         */
        using NumBytesType = uint32_t;

        /**
         * The starting offset for the number of bytes in the OutputRow.  This will immediately succeed the size of the header value.
         */
        METAL_CONSTANT static constexpr auto NumBytesOffset = details::DetermineAlignment<NumBytesType>(sizeof(SizeOfHeaderType) + SizeOfHeaderOffset);

        /**
         * The type to store the number of columns in the OutputRow.
         */
        using NumColumnsType = uint8_t;

        /**
         * The starting offset for the number of columns in the OutputRow.  This will immediately succeed the number of bytes value.
         */
        METAL_CONSTANT static constexpr auto NumColumnsOffset = details::DetermineAlignment<NumColumnsType>(sizeof(NumBytesType) + NumBytesOffset);

        /**
         * The starting offset for the column types for each of the columns.  There will be `NumColumn` values.  The first one will immediately
         * succeed the number of columns value.
         */
        METAL_CONSTANT static constexpr auto ColumnTypeOffset = details::DetermineAlignment<ColumnType>(sizeof(NumColumnsType) + NumColumnsOffset);

        /**
         * The type to store the column size.  The column size will only be stored if that column is a variable size.
         *
         * @see ColumnVariableSize
         * @see TempRow::ColumnSize
         */
        using ColumnSizeType = StringSection::SizeType;

        /**
         * The type to store the number of rows.  Note this value is not written in the OutputRow directly.
         *
         * This type is only used by parsers.
         */
        using NumRowsType = uint32_t;

        /**
         * Returns the size of the header given a number of columns.
         * @param numColumns The number of columns in the OutputRow.
         */
        CPP_CONST_FUNC static SizeOfHeaderType SizeOfHeader(NumColumnsType numColumns) CPP_NOEXCEPT {
            auto sizeOfHeader = ColumnTypeOffset;
            sizeOfHeader += sizeof(ColumnType) * numColumns;
            return sizeOfHeader;
        }
    };
}
