//
//  temp_row.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-04-02.
//

#pragma once

#include "constants.h"
#include "column_type.h"
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

        class TempRowBuilder {
        public:
            constexpr METAL_CONSTANT static size_t MAX_VALUE = 256;
            TempRowBuilder() = default;

            ColumnType columnTypes[MAX_VALUE];
            uint8_t columnSizes[MAX_VALUE];
            uint8_t numColumns = 0;
        };

        TempRow(TempRowBuilder builder) {
            // Start at 1, because the first thing is the length of the header!
            uint8_t lengthOfHeader = 1;

            {
                // Write length of buffer
                lengthOfHeader += sizeof(uint32_t);
            }
            {
                // Write num columns
                this->_data[1] = builder.numColumns;
                lengthOfHeader++;
            }
            {
                // Column types
                for (auto i = 0; i < builder.numColumns; ++i) {
                    this->_data[2 + i] = (value_type) builder.columnTypes[i];
                    lengthOfHeader++;
                }
            }
            {
                // Column sizes (for all variable columns)
                for (auto i = 0; i < builder.numColumns; ++i) {
                    auto columnType = (enum ColumnType) builder.columnTypes[i];
                    if (columnType == String) {
                        // Hack using the length of the header so we dynamically write to the correct place.
                        this->_data[lengthOfHeader++] = (value_type) builder.columnSizes[i];
                    }
                }
            }

            this->_data[0] = lengthOfHeader;
        }

        TempRow() = default;

        uint8_t LengthOfHeader() const {
            return this->_data[0];
        }

        size_t SizeOfPartialRow() const {
            size_t sum = this->size();
            for (size_t i = 0; i < this->NumColumns(); ++i) {
                if (this->ColumnVariableSize(i)) {
                    sum += sizeof(this->ColumnSize(i));
                }
            }
            return sum;
        }

        uint8_t NumColumns() const {
            constexpr auto index = sizeof(this->LengthOfHeader());
            return this->_data[index];
        }

        enum ColumnType ColumnType(size_t column) const {
            constexpr auto index = sizeof(this->LengthOfHeader()) + sizeof(this->NumColumns());
            return (enum ColumnType) this->_data[index + column];
        }

        bool ColumnVariableSize(size_t column) const {
            switch (this->ColumnType(column)) {
            case String:
                return true;
            case Float:
            case Integer:
            case Unknown:
                return false;
            }
        }

        uint8_t ColumnSize(size_t column) const {
            // Calculate column size of variable sizes
            switch (this->ColumnType(column)) {
            case String:
                break;
            case Float:
                return sizeof(types::FloatType);
            case Integer:
                return sizeof(types::IntegerType);
            case Unknown:
                return 0;
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
                /* fixed header */ sizeof(this->LengthOfHeader()) + sizeof(this->NumColumns()) +
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

        LocalStringSection ReadColumnString(size_t column) const {
            const auto startOffset = this->ColumnStartOffset(column);
            const auto columnSize = this->ColumnSize(column);
            return LocalStringSection(this->data(startOffset), columnSize);
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
    };
}
