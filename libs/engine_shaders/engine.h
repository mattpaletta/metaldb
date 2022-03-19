#pragma once

#include "constants.h"
#include "strings.h"

#ifndef __METAL__
#include <cstdint>
#endif

#define MAX_VM_STACK_SIZE 100

namespace metaldb {
    class RawTable {
    public:
        RawTable(METAL_DEVICE char* rawData) : _rawData(rawData) {}

        METAL_CONSTANT static constexpr int8_t RAW_DATA_NUM_ROWS_INDEX = 2;

        int8_t GetSizeOfHeader() {
            return (int8_t) _rawData[0];
        }

        int8_t GetSizeOfData() {
            return (int8_t) _rawData[1];
        }

        int8_t GetNumRows() {
            return (int8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX];
        }

        int8_t GetRowIndex(int8_t index) {
            return (int8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX + 1 + index];
        }

        int8_t GetStartOfData() {
            return GetSizeOfHeader() + 1;
        }

        METAL_DEVICE char* data(uint8_t index = 0) {
            return &this->_rawData[index];
        }

    private:
        METAL_DEVICE char* _rawData;
    };

    using instruction_serialized_value_type = int8_t;
    enum InstructionType : instruction_serialized_value_type {
        PARSEROW,
        PROJECTION,
    };

    enum ColumnType : instruction_serialized_value_type {
        String,
        Float,
        Integer
    };

    enum Method {
        CSV
    };

    class StringSection final {
    public:
        StringSection(METAL_DEVICE char* str, uint8_t size) : _size(size), _str(str) {}

        uint8_t size() const {
            return this->_size;
        }

        METAL_DEVICE char* str() const {
            return this->_str;
        }

    private:
        uint8_t _size;
        METAL_DEVICE char* _str;
    };

    class ParseRowInstruction final {
    public:
        // Pointer points to beginning of ParseRow instruction.
        ParseRowInstruction(METAL_DEVICE int8_t* instructions) : _instructions(instructions) {}

        Method getMethod() const {
            return (Method) this->_instructions[0];
        }

        int8_t numColumns() const {
            return (int8_t) this->_instructions[1];
        }

        ColumnType getColumnType(int8_t index) const {
            // +2 because of the other two functions that are first in the serialization.
            return (ColumnType) this->_instructions[index + 2];
        }

        StringSection readCSVColumn(RawTable METAL_THREAD & rawTable, uint8_t row, uint8_t column) const {
            auto rowIndex = rawTable.GetRowIndex(row);

            // Get the column by scanning
            METAL_DEVICE char* startOfColumn = rawTable.data(rowIndex);
            for (uint8_t i = 0; i < column && startOfColumn; ++i) {
                startOfColumn = metal::strings::strchr(startOfColumn, ',');
            }

            if (!startOfColumn) {
                return StringSection(startOfColumn, 0);
            }

            METAL_DEVICE char* endOfColumn = metal::strings::strchr(startOfColumn, ',');

            return StringSection(startOfColumn, endOfColumn - startOfColumn);
        }

    private:
        METAL_DEVICE int8_t* _instructions;
    };

    class VM {
    public:
        VM() = default;
        ~VM() = default;

        void run(METAL_DEVICE int8_t* instructions) {
            const auto numInstructions = instructions[0];

            uint8_t index = 1;
            for (uint8_t i = 0; i < numInstructions; ++i) {
                switch (VM::decodeType(instructions, index)) {
                case PARSEROW: {
                    this->runParseRowInstruction(&instructions[index]);
                    break;
                }
                case PROJECTION: {
                    this->runProjectionInstruction(&instructions[index]);
                    break;
                }
                }
            }
        }

    private:
        static void runParseRowInstruction(METAL_DEVICE int8_t* instructions) {

        }

        static void runProjectionInstruction(METAL_DEVICE int8_t* instructions) {

        }

        static InstructionType decodeType(METAL_DEVICE int8_t* instructions, uint8_t METAL_THREAD& index) {
            return (InstructionType) instructions[index++];
        }

        uint8_t stack[MAX_VM_STACK_SIZE];
    };
}

inline void runQueryKernelImpl(METAL_DEVICE char* rawData, METAL_DEVICE int8_t* instructions, uint8_t thread_position_in_grid) {
    metaldb::RawTable rawTable(rawData);

    // TODO: Build out VM & run it through the rows.

}

#ifdef __METAL__
kernel void runQueryKernel(device char* rawData [[ buffer(0) ]], device int8_t* instructions [[ buffer(1) ]], uint id [[ thread_position_in_grid ]]) {
    runQueryKernelImpl(rawData, instructions, id);
}
#endif

