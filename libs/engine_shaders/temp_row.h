//
//  temp_row.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-04-02.
//

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

        using SizeOfHeaderType = OutputRow::SizeOfHeaderType;
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = 0;

        using NumColumnsType = OutputRow::NumColumnsType;
        METAL_CONSTANT static constexpr auto NumColumnsOffset = sizeof(SizeOfHeaderType) + SizeOfHeaderOffset;

        METAL_CONSTANT static constexpr auto ColumnTypeOffset = sizeof(NumColumnsType) + NumColumnsOffset;

        using ColumnSizeType = OutputRow::ColumnSizeType;

        class TempRowBuilder {
        public:
            constexpr METAL_CONSTANT static size_t MAX_VALUE = 256;
            TempRowBuilder() = default;

            ColumnType columnTypes[MAX_VALUE];
            ColumnSizeType columnSizes[MAX_VALUE];
            NumColumnsType numColumns = 0;
        };

        TempRow(TempRowBuilder builder) {
            // Start at 1, because the first thing is the length of the header!
            SizeOfHeaderType lengthOfHeader = 1;

            {
                // Write length of buffer
                lengthOfHeader += sizeof(SizeOfHeaderType);
            }
            {
                {
                    // Write num columns
                    auto numColumns = builder.numColumns;
                    for (size_t n = 0; n < sizeof(NumColumnsType); ++n) {
                        this->_data[NumColumnsOffset + n] = (int8_t)(numColumns >> (8 * n)) & 0xff;
                    }
                    lengthOfHeader += sizeof(NumColumnsType);
                }
            }
            {
                // Column types
                for (auto i = 0; i < builder.numColumns; ++i) {
                    auto columnType = builder.columnTypes[i];
                    this->_data[ColumnTypeOffset + i] = columnType;
                }
                lengthOfHeader += builder.numColumns;
            }
            {
                // Column sizes (for all variable columns)
                for (auto i = 0; i < builder.numColumns; ++i) {
                    auto columnType = (enum ColumnType) builder.columnTypes[i];
                    if (columnType == String) {
                        // Hack using the length of the header so we dynamically write to the correct place.
                        auto columnSize = builder.columnSizes[i];
                        for (size_t n = 0; n < sizeof(ColumnSizeType); ++n) {
                            this->_data[lengthOfHeader++] = (int8_t)(columnSize >> (8 * n)) & 0xff;
                        }
                    }
                }
            }

            *((SizeOfHeaderType METAL_THREAD *) &(this->_data[SizeOfHeaderOffset])) = lengthOfHeader;
        }

        TempRow() = default;

        SizeOfHeaderType LengthOfHeader() const {
            return *((SizeOfHeaderType METAL_THREAD *) &(this->_data[SizeOfHeaderOffset]));
        }

        size_t SizeOfPartialRow() const {
            size_t sum = this->size();
            for (auto i = 0; i < this->NumColumns(); ++i) {
                if (this->ColumnVariableSize(i)) {
                    sum += sizeof(this->ColumnSize(i));
                }
            }
            return sum;
        }

        NumColumnsType NumColumns() const {
            constexpr auto index = sizeof(this->LengthOfHeader());
            return *((NumColumnsType METAL_THREAD *) &(this->_data[index]));
        }

        enum ColumnType ColumnType(size_t column) const {
            return (enum ColumnType) this->_data[ColumnTypeOffset + column];
        }

        bool ColumnVariableSize(size_t column) const {
            return metaldb::ColumnVariableSize(this->ColumnType(column));
        }

        ColumnSizeType ColumnSize(size_t column) const {
            // Calculate column size of variable sizes
            {
                auto columnType = this->ColumnType(column);
                if (!metaldb::ColumnVariableSize(columnType)) {
                    return metaldb::BaseColumnSize(columnType);
                }
            }
            size_t offsetOfVariableLength = 0;
            for (size_t i = 0; i < column; ++i) {
                switch (this->ColumnType(i)) {
                case String: {
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
            return this->_data[
                /* fixed header */ColumnTypeOffset +
                /* columnType offset */ this->NumColumns() * sizeof(this->ColumnType(0)) +
                /* read into column size */ offsetOfVariableLength];
        }

        size_t ColumnStartOffset(size_t column) const {
            size_t sum = 0;
            for (size_t i = 0; i < column; ++i) {
                sum += this->ColumnSize(i);
            }
            return sum;
        }

        LocalStringSection getString() {
            return LocalStringSection(this->data(), this->_size);
        }

        types::FloatType ReadColumnFloat(size_t column) const {
            const auto startOffset = this->ColumnStartOffset(column);
            return *((types::FloatType METAL_THREAD *)(this->data(startOffset)));
        }

        types::IntegerType ReadColumnInt(size_t column) const {
            const auto startOffset = this->ColumnStartOffset(column);
            return *((types::IntegerType METAL_THREAD *)(this->data(startOffset)));
        }

        ConstLocalStringSection ReadColumnString(size_t column) const {
            const auto startOffset = this->ColumnStartOffset(column);
            const auto columnSize = this->ColumnSize(column);
            return ConstLocalStringSection(this->data(startOffset), columnSize);
        }

        METAL_THREAD char* data(size_t index = 0) {
            return &this->_data[this->LengthOfHeader() + index];
        }

        const METAL_THREAD char* data(size_t index = 0) const {
            return &this->_data[this->LengthOfHeader() + index];
        }

        METAL_THREAD char* end() {
            return this->data(this->_size);
        }

        size_t size() const {
            return this->_size;
        }

        void append(METAL_THREAD char* str, size_t len) {
            metal::strings::strncpy(this->end(), str, len);
            this->_size += len;
        }

        void append(types::IntegerType val) {
            this->appendImpl(val);
        }

        void append(types::FloatType val) {
            this->appendImpl(val);
        }

#ifdef __METAL__
        // Supports copying from device to local storage
        void append(METAL_DEVICE char* str, size_t len) {
            metal::strings::strncpy(this->end(), str, len);
            this->_size += len;
        }
#endif

    private:
        METAL_THREAD value_type _data[MAX_OUTPUT_ROW_LENGTH];
        size_t _size = 0;

        template<typename T>
        void appendImpl(T val) {
            this->append((char METAL_THREAD *) &val, sizeof(T));
        }
    };
}
