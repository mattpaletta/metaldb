//
//  output_instruction.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-31.
//

#pragma once

#include "constants.h"
#include "db_constants.h"
#include "output_row.h"
#include "strings.h"
#include "PrefixSum.h"

namespace metaldb {
    class OutputInstruction final {
    public:
        OutputInstruction(InstSerializedValuePtr instructions) : _instructions(instructions) {}

        InstSerializedValuePtr end() const {
            // Returns 1 past the end of the instruction
            const int8_t offset = 1;
            return &this->_instructions[offset];
        }

        void WriteRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) {
            // Write row into output.

            /**
             * Size of header (4 bytes)
             * Num bytes (4 bytes)
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
            PrefixScanKernel<DbConstants::MAX_NUM_ROWS, OutputRow::NumBytesType>(constants.rowSizeScratch, rowSize, constants.thread_position_in_threadgroup, constants.thread_execution_width);

            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
            auto startIndex = constants.rowSizeScratch[index];
            threadgroup_barrier(metal::mem_flags::mem_none);

            const auto bufferSize = ThreadGroupReduceCooperativeAlgorithm<DbConstants::MAX_NUM_ROWS, OutputRow::NumBytesType>(constants.rowSizeScratch, rowSize, constants.thread_position_in_threadgroup, constants.thread_execution_width);

            // Needs this threadgroup barrier, otherwise causes internal compiler error :/
            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
#endif
            if (isFirstThread) {
                // Only the first thread should write the header, all other threads wait

                // Write length of header
                // First byte is the length of the header.
                OutputRow::SizeOfHeaderType lengthOfHeader = sizeof(OutputRow::SizeOfHeaderType);
#ifdef __METAL__
                // Write length of buffer
                {
                    for (size_t n = 0; n < sizeof(OutputRow::NumBytesType); ++n) {
                        constants.outputBuffer[sizeof(OutputRow::SizeOfHeaderType) + n] = (int8_t)(bufferSize >> (8 * n)) & 0xff;
                        lengthOfHeader++;
                    }
                }
#endif

                // Write the number of columns
                {
                    auto numColumns = row.NumColumns();
                    if (sizeof(OutputRow::NumColumnsType) == 1) {
                        constants.outputBuffer[lengthOfHeader++] = numColumns;
                    } else {
                        for (size_t n = 0; n < sizeof(OutputRow::NumColumnsType); ++n) {
                            constants.outputBuffer[lengthOfHeader++] = (int8_t)(numColumns >> (8 * n)) & 0xff;
                        }
                    }
                }


                // Write the types of each column
                for (size_t i = 0; i < row.NumColumns(); ++i) {
                    constants.outputBuffer[lengthOfHeader++] = (InstSerializedValue) row.ColumnType(i);
                }

                // Write the size of the header
                if (sizeof(OutputRow::SizeOfHeaderType) == 1) {
                    constants.outputBuffer[OutputRow::SizeOfHeaderOffset] = lengthOfHeader;
                } else {
                    for (size_t n = 0; n < sizeof(OutputRow::SizeOfHeaderType); ++n) {
                        constants.outputBuffer[OutputRow::SizeOfHeaderOffset + n] = (int8_t)(lengthOfHeader >> (8 * n)) & 0xff;
                    }
                }

#ifdef __METAL__
                // First thread starts after the header
                startIndex += lengthOfHeader;
#endif
            }
#ifndef __METAL__
            // Start at the length of the buffer
            size_t startIndex = *(OutputRow::SizeOfHeaderType METAL_DEVICE *)(&constants.outputBuffer[1]);
#endif

            size_t nextAvailableSlot = startIndex;

            // Write the column sizes for all non-zero size columns
            for (size_t i = 0; i < row.NumColumns(); ++i) {
                if (row.ColumnVariableSize(i)) {
                    auto rowSize = row.ColumnSize(i);
                    if (sizeof(OutputRow::ColumnSizeType) == 1) {
                        constants.outputBuffer[nextAvailableSlot++] = rowSize;
                    } else {
                        for (size_t n = 0; n < sizeof(OutputRow::SizeOfHeaderType); ++n) {
                            constants.outputBuffer[nextAvailableSlot++] = (int8_t)(rowSize >> (8 * n)) & 0xff;
                        }
                    }
                }
            }

            // Write the data for the row
            for (size_t i = 0; i < row.size(); ++i) {
                const auto value = row.data()[i];
                constants.outputBuffer[nextAvailableSlot++] = value;
            }

#ifndef __METAL__
            // Update the size of the buffer
            *(OutputRow::SizeOfHeaderType METAL_DEVICE *)(&constants.outputBuffer[1]) = nextAvailableSlot;
#endif
        }

    private:
        InstSerializedValuePtr _instructions;
    };
}
