//
//  output_instruction.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-31.
//

#pragma once

#include "constants.h"
#include "db_constants.h"
#include "strings.h"
#include "PrefixSum.h"

namespace metaldb {
    class OutputInstruction final {
    public:
        OutputInstruction(int8_t METAL_DEVICE * instructions) : _instructions(instructions) {}

        METAL_DEVICE int8_t* end() const {
            // Returns 1 past the end of the instruction
            const int8_t offset = 1;
            return &this->_instructions[offset];
        }

        void WriteRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) {
            // Write row into output.

            /**
             * Size of header
             * Num bytes (2 bytes)
             * Num Rows
             * Column Types
             * --------
             * Column Types for row (if string)
             * Row data
             * --------
             */
            const auto index = constants.thread_position_in_grid;
            const auto isFirstThread = index == 0;
#ifdef __METAL__
            // Sync here
            threadgroup_barrier(metal::mem_flags::mem_threadgroup);

            int8_t rowSize = row.SizeOfPartialRow();
            if (isFirstThread) {
                rowSize += row.LengthOfHeader();
            }

            threadgroup_barrier(metal::mem_flags::mem_threadgroup);

            // Do the prefix sum within the threadgroup
            PrefixScanKernel<DbConstants::MAX_NUM_ROWS, uint32_t>(constants.rowSizeScratch, rowSize, constants.thread_position_in_threadgroup, constants.thread_execution_width);

            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
            auto startIndex = constants.rowSizeScratch[index];
            threadgroup_barrier(metal::mem_flags::mem_none);

            const auto bufferSize = ThreadGroupReduceCooperativeAlgorithm<DbConstants::MAX_NUM_ROWS, uint32_t>(constants.rowSizeScratch, rowSize, constants.thread_position_in_threadgroup, constants.thread_execution_width);

            // Needs this threadgroup barrier, otherwise causes internal compiler error :/
            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
#endif
            if (isFirstThread) {
                // Only the first thread should write the header, all other threads wait

                // Write length of header
                // First byte is the length of the header.
                uint16_t lengthOfHeader = 1;
#ifdef __METAL__
                // Write length of buffer (2 bytes)
                {
                    for (size_t n = 0; n < sizeof(uint32_t); ++n) {
                        constants.outputBuffer[1 + n] = (int8_t)(bufferSize >> (8 * n)) & 0xff;
                        lengthOfHeader++;
                    }
                }
#endif

                // Write the number of columns
                constants.outputBuffer[lengthOfHeader++] = row.NumColumns();

                // Write the types of each column
                for (size_t i = 0; i < row.NumColumns(); ++i) {
                    constants.outputBuffer[lengthOfHeader++] = (instruction_serialized_value_type) row.ColumnType(i);
                }

                // Write the size of the header
                constants.outputBuffer[0] = lengthOfHeader;

#ifdef __METAL__
                // First thread starts after the header
                startIndex += lengthOfHeader;
#endif
            }
#ifndef __METAL__
            // Start at the length of the buffer
            size_t startIndex = *(uint16_t METAL_DEVICE *)(&constants.outputBuffer[1]);
#endif

            size_t nextAvailableSlot = startIndex;

            // Write the column sizes for all non-zero size columns
            for (size_t i = 0; i < row.NumColumns(); ++i) {
                if (row.ColumnVariableSize(i)) {
                    constants.outputBuffer[nextAvailableSlot++] = row.ColumnSize(i);
                }
            }

            // Write the data for the row
            for (size_t i = 0; i < row.size(); ++i) {
                const auto value = row.data()[i];
                constants.outputBuffer[nextAvailableSlot++] = value;
            }

#ifndef __METAL__
            // Update the size of the buffer
            *(uint16_t METAL_DEVICE *)(&constants.outputBuffer[1]) = nextAvailableSlot;
#endif
        }

    private:
        METAL_DEVICE int8_t* _instructions;
    };
}
