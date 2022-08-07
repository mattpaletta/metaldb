#pragma once

#include "constants.h"
#include "column_type.h"
#include "output_row.h"
#include "string_section.h"
#include "strings.h"

namespace metaldb {
    /**
     * Stores a row being processed by the database.
     *
     * The beginning contains a header with information about the columns in the row.
     * ------------------
     * Length of Header
     * Num Columns - max columns 256
     * Column Type 1....N
     * Column sizes (for all variable size columns) (1....N) - max size 256
     * ------------------
     * Column 1 Data....
     *
     */
    class TempRow {
    public:
        using value_type = char;
        
        /**
         * The type to store the size of the header.
         */
        using SizeOfHeaderType = OutputRow::SizeOfHeaderType;
        
        /**
         * The starting offset for the size of the header.
         */
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;
        
        /**
         * The type to store the number of columns.
         */
        using NumColumnsType = OutputRow::NumColumnsType;
        
        /**
         * The starting offset for the number of columns.
         */
        METAL_CONSTANT static constexpr auto NumColumnsOffset = sizeof(SizeOfHeaderType) + SizeOfHeaderOffset;
        
        /**
         * The starting offset for the types of each column.
         */
        METAL_CONSTANT static constexpr auto ColumnTypeOffset = sizeof(NumColumnsType) + NumColumnsOffset;
        
        /**
         * The type to store the size of each column.
         */
        using ColumnSizeType = OutputRow::ColumnSizeType;
        
        /**
         * The general type to use for sizes.
         */
        using SizeType = types::SizeType;
        
        /**
         * A helper class to assist with building a TempRow object.
         */
        class TempRowBuilder {
        public:
            constexpr METAL_CONSTANT static SizeType MAX_VALUE = 256;
            TempRowBuilder() = default;
            
            ColumnType columnTypes[MAX_VALUE];
            ColumnSizeType columnSizes[MAX_VALUE];
            NumColumnsType numColumns = 0;
        };
        
        /**
         * Constructs a new TempRow from a @b TempRowBuilder .
         */
        TempRow(TempRowBuilder builder) {
            SizeOfHeaderType lengthOfHeader = sizeof(SizeOfHeaderType);
            
            {
                // Write num columns
                auto numColumns = builder.numColumns;
                WriteBytesStartingAt(&this->_data[NumColumnsOffset], numColumns);
                lengthOfHeader += sizeof(numColumns);
            }
            {
                // Column types
                for (auto i = 0; i < builder.numColumns; ++i) {
                    auto columnType = builder.columnTypes[i];
                    WriteBytesStartingAt(&this->_data[ColumnTypeOffset + (i * sizeof(enum ColumnType))], columnType);
                }
                lengthOfHeader += (builder.numColumns * sizeof(enum ColumnType));
            }
            {
                // Column sizes (for all variable columns)
                for (auto i = 0; i < builder.numColumns; ++i) {
                    auto columnType = (enum ColumnType) builder.columnTypes[i];
                    if (metaldb::ColumnVariableSize(columnType)) {
                        // Hack using the length of the header so we dynamically write to the correct place.
                        auto columnSize = builder.columnSizes[i];
                        WriteBytesStartingAt(&this->_data[lengthOfHeader], columnSize);
                        lengthOfHeader += sizeof(columnSize);
                    }
                }
            }
            
            WriteBytesStartingAt(&this->_data[SizeOfHeaderOffset], lengthOfHeader);
        }
        
        /**
         * Constructs a blank TempRow
         */
        TempRow() = default;
        
        /**
         * Returns the length of the header in bytes.
         */
        SizeOfHeaderType LengthOfHeader() const {
            return ReadBytesStartingAt<SizeOfHeaderType>(&this->_data[SizeOfHeaderOffset]);
        }
        
        /**
         * Returns the size of the data portion of the TempRow plus storing the sizes of each variable column.
         * This is intended for use when constructing an @b OutputRow .
         *
         * @see OutputInstruction::WriteRow
         */
        SizeType SizeOfPartialRow() const {
            auto sum = this->Size();
            for (auto i = 0; i < this->NumColumns(); ++i) {
                if (this->ColumnVariableSize(i)) {
                    sum += sizeof(this->ColumnSize(i));
                }
            }
            return sum;
        }
        
        /**
         * Returns the number of columns.
         */
        NumColumnsType NumColumns() const {
            return ReadBytesStartingAt<NumColumnsType>(&this->_data[NumColumnsOffset]);
        }
        
        /**
         * Returns the type of the column at a particular index.
         */
        enum ColumnType ColumnType(NumColumnsType column) const {
            return ReadBytesStartingAt<enum ColumnType>(&this->_data[ColumnTypeOffset + (column * sizeof(enum ColumnType))]);
        }
        
        /**
         * Returns true if the column can be a variable size.
         */
        bool ColumnVariableSize(NumColumnsType column) const {
            return metaldb::ColumnVariableSize(this->ColumnType(column));
        }
        
        /**
         * Returns the actual size of a column, including columns with variable size.
         */
        ColumnSizeType ColumnSize(NumColumnsType column) const {
            // Calculate column size of variable sizes
            {
                auto columnType = this->ColumnType(column);
                if (!metaldb::ColumnVariableSize(columnType)) {
                    return metaldb::BaseColumnSize(columnType);
                }
            }
            NumColumnsType offsetOfVariableLength = 0;
            for (size_t i = 0; i < column; ++i) {
                switch (this->ColumnType(i)) {
                case String:
                case String_opt:
                case Float_opt:
                case Integer_opt: {
                    offsetOfVariableLength++;
                    break;
                }
                case Float:
                case Integer:
                case Unknown:
                    continue;
                }
            }
            // Lookup in the index of column type
            return ReadBytesStartingAt<ColumnSizeType>(&this->_data[
                /* fixed header */ColumnTypeOffset +
                                  /* columnType offset */ this->NumColumns() * sizeof(this->ColumnType(0)) +
                                  /* read into column size */ offsetOfVariableLength]);
        }
        
        /**
         * Returns the starting data offset for a column.
         */
        SizeType ColumnStartOffset(NumColumnsType column) const {
            SizeType sum = 0;
            for (SizeType i = 0; i < column; ++i) {
                sum += this->ColumnSize(i);
            }
            return sum;
        }
        
        /**
         * Returns the @b TempRow as a string.
         */
        LocalStringSection GetString() {
            return LocalStringSection(this->Data(), this->_size);
        }
        
        /**
         * Returns true if the column is not nullable or if it is nullable, but has a value.
         */
        bool HasValue(NumColumnsType column) const {
            if (this->IsNullable(column)) {
                return this->ColumnSize(column) > 0;
            } else {
                return true;
            }
        }
        
        /**
         * Returns true if the column could be null.
         */
        bool IsNullable(NumColumnsType column) const {
            switch(this->ColumnType(column)) {
            case String_opt:
            case Float_opt:
            case Integer_opt:
            case Unknown:
                return true;
            case String:
            case Float:
            case Integer:
                return false;
            }
        }
        
        /**
         * Reads a column as a float.
         * It is up to the caller to ensure the @b ColumnType is a float.
         */
        types::FloatType ReadColumnFloat(NumColumnsType column) const {
            const auto startOffset = this->ColumnStartOffset(column);
            return ReadBytesStartingAt<types::FloatType>(this->Data() + startOffset);
        }
        
        /**
         * Reads a column as an integer.
         * It is up to the caller to ensure the @b ColumnType is an integer.
         */
        types::IntegerType ReadColumnInt(NumColumnsType column) const {
            const auto startOffset = this->ColumnStartOffset(column);
            return ReadBytesStartingAt<types::IntegerType>(this->Data() + startOffset);
        }
        
        /**
         * Reads a column as a string.
         * It is up to the caller to ensure the @b ColumnType is a string.
         */
        ConstLocalStringSection ReadColumnString(NumColumnsType column) const {
            const auto startOffset = this->ColumnStartOffset(column);
            const auto columnSize = this->ColumnSize(column);
            return ConstLocalStringSection(this->Data(startOffset), columnSize);
        }
        
        /**
         * Returns a mutable pointer into the data section at a given offset.
         */
        METAL_THREAD value_type* Data(SizeType index = 0) {
            return &this->_data[this->LengthOfHeader() + index];
        }
        
        /**
         * Returns a const pointer into the data section at a given offset.
         */
        const METAL_THREAD value_type* Data(SizeType index = 0) const {
            return &this->_data[this->LengthOfHeader() + index];
        }
        
        /**
         * Returns a pointer one byte past the data section.
         */
        METAL_THREAD value_type* End() {
            return this->Data(this->_size);
        }
        
        /**
         * Returns the current size of the data section of the @b TempRow
         */
        SizeType Size() const {
            return this->_size;
        }
        
        /**
         * Appends a string to the data section.
         */
        void Append(METAL_THREAD char* str, SizeType len) {
            metal::strings::strncpy(this->End(), str, len);
            this->_size += len;
        }
        
        /**
         * Appends an integer to the data section.
         */
        void Append(types::IntegerType val) {
            this->AppendImpl(val);
        }
        
        /**
         * Appends a float to the data section.
         */
        void Append(types::FloatType val) {
            this->AppendImpl(val);
        }
        
#ifdef __METAL__
        /**
         * Appends a string to the data section.
         */
        void Append(METAL_DEVICE char* str, ColumnSizeType len) {
            metal::strings::strncpy(this->End(), str, len);
            this->_size += len;
        }
#endif
        
    private:
        mutable METAL_THREAD value_type _data[MAX_OUTPUT_ROW_LENGTH];
        SizeType _size = 0;
        
        template<typename T>
        void AppendImpl(T val) {
            this->Append((char METAL_THREAD *) &val, sizeof(T));
        }
    };
}
