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
                    auto columnType = (ColumnType) builder.columnTypes[i];
                    if (columnType == String) {
                        // Hack using the length of the header so we dynamically write to the correct place.
                        this->_data[lengthOfHeader++] = builder.columnSizes[i];
                    }
                }
            }

            this->_data[0] = lengthOfHeader;
        }

        TempRow() = default;

        LocalStringSection getString() {
            return LocalStringSection(&this->_data[0], this->_size);
        }

        METAL_THREAD char* end() {
            return &this->_data[this->_size];
        }

        void append(METAL_THREAD char* str, size_t len) {
            metal::strings::strncpy(this->end(), str, len);
        }

#ifdef __METAL__
        // Supports copying from device to local storage
        void append(METAL_DEVICE char* str, size_t len) {
            metal::strings::strncpy(this->end(), str, len);
        }
#endif

    private:
        METAL_THREAD value_type _data[MAX_OUTPUT_ROW_LENGTH];
        uint8_t _size = 0;
    };
}
