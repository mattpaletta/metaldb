//
//  temp_row.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-04-02.
//

#pragma once

#include "constants.h"
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

            uint8_t numColumns;
            ColumnType columnTypes[MAX_VALUE];
            uint8_t columnSizes[MAX_VALUE];
        };

        TempRow(TempRowBuilder builder) {
            uint8_t lengthOfHeader = 0;

            {
                // Write num columns
                this->_data[1] = builder.numColumns;
                lengthOfHeader++;
            }
            {
                // Column types
                for (uint8_t i = 0; i < builder.numColumns; ++i) {
                    this->_data[2 + i] = (value_type) builder.columnTypes[i];
                    lengthOfHeader++;
                }
            }
            {
                // Column sizes (for all variable columns)
                for (uint8_t i = 0; i < builder.numColumns; ++i) {
                    auto columnType = (enum ColumnType) builder.columnTypes[i];
                    if (columnType == String) {
                        // Hack using the length of the header so we dynamically write to the correct place.
                        this->_data[lengthOfHeader++] = builder.columnSizes[i];
                    }
                }
            }

            this->_data[0] = lengthOfHeader;
        }

        TempRow() = default;

        uint8_t LengthOfHeader() const {
            return this->_data[0];
        }

        size_t NumColumns() const {
            return this->_data[1];
        }

        enum ColumnType ColumnType(size_t column) {
            return (enum ColumnType) this->_data[2 + column];
        }

        uint8_t ColumnSize(size_t column) {
            // Calculate column size of variable sizes
            switch (this->ColumnType(column)) {
            case String:
                break;
            case Float:
                return sizeof(float);
            case Integer:
                return sizeof(uint8_t);
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
                    continue;
                }
            }
            // Lookup in the index of column type
            return this->_data[
                /* fixed header */ 2 +
                /* columnType offset */ this->NumColumns() +
                /* read into column size */ offsetOfVariableLength];
        }

        size_t ColumnStartOffset(size_t column) {
            size_t sum = 0;
            for (size_t i = 0; i < column; ++i) {
                sum += this->ColumnSize(i);
            }
            return sum;
        }

        LocalStringSection getString() {
            return LocalStringSection(&this->_data[this->LengthOfHeader()], this->_size);
        }

        METAL_THREAD char* data(size_t index = 0) {
            return &this->_data[index];
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
