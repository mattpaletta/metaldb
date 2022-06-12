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
        using SizeOfHeaderType = OutputRow::SizeOfHeaderType;
        METAL_CONSTANT static constexpr auto SizeOfHeaderOffset = OutputRow::SizeOfHeaderOffset;

        using NumBytesType = OutputRow::NumBytesType;
        METAL_CONSTANT static constexpr auto NumBytesOffset = OutputRow::NumBytesOffset;

        using NumColumnsType = OutputRow::NumColumnsType;
        METAL_CONSTANT static constexpr auto NumColumnsOffset = OutputRow::NumColumnsOffset;

        METAL_CONSTANT static constexpr auto ColumnTypeOffset = OutputRow::ColumnTypeOffset;

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
            PrefixScanKernel<DbConstants::MAX_NUM_ROWS, NumBytesType>(constants.rowSizeScratch, rowSize, constants.thread_position_in_threadgroup, constants.thread_execution_width);

            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
            auto startIndex = constants.rowSizeScratch[index];
            threadgroup_barrier(metal::mem_flags::mem_none);

            const auto bufferSize = ThreadGroupReduceCooperativeAlgorithm<DbConstants::MAX_NUM_ROWS, NumBytesType>(constants.rowSizeScratch, rowSize, constants.thread_position_in_threadgroup, constants.thread_execution_width);

            // Needs this threadgroup barrier, otherwise causes internal compiler error :/
            threadgroup_barrier(metal::mem_flags::mem_threadgroup);
#endif
            if (isFirstThread) {
                // Only the first thread should write the header, all other threads wait

                // Write length of header
                // First byte is the length of the header.
                SizeOfHeaderType lengthOfHeader = sizeof(SizeOfHeaderType);

#ifndef __METAL__
                // Placeholder
                NumBytesType bufferSize = 0;
#endif
                // Write length of buffer
#ifndef __METAL__
                std::cout << "Writing Num Bytes at: " << NumBytesOffset << " with value: " << bufferSize << std::endl;
#endif
                WriteBytesStartingAt(&constants.outputBuffer[NumBytesOffset], bufferSize);
                lengthOfHeader += sizeof(NumBytesType);

                // Write the number of columns
                auto numColumns = row.NumColumns();
#ifndef __METAL__
                std::cout << "Writing Num Columns at: " << NumColumnsOffset << " with value: " << (int) numColumns << std::endl;
#endif
                WriteBytesStartingAt(&constants.outputBuffer[NumColumnsOffset], numColumns);
                lengthOfHeader += sizeof(NumColumnsType);

                // Write the types of each column
                for (auto i = 0; i < row.NumColumns(); ++i) {
                    auto columnType = row.ColumnType(i);
#ifndef __METAL__
                    std::cout << "Writing Column Type at: " << lengthOfHeader << " with value: " << (int) columnType << std::endl;
#endif
                    WriteBytesStartingAt(&constants.outputBuffer[lengthOfHeader], columnType);
                    lengthOfHeader += sizeof(ColumnType);
                }

                // Write the size of the header
#ifndef __METAL__
                std::cout << "Writing Length of header at: " << SizeOfHeaderOffset << " with value: " << lengthOfHeader << std::endl;
#endif
                WriteBytesStartingAt(&constants.outputBuffer[SizeOfHeaderOffset], lengthOfHeader);
#ifdef __METAL__
                // First thread starts after the header
                startIndex += lengthOfHeader;
#endif
            }
#ifndef __METAL__
            // Start at the length of the buffer
            size_t startIndex = [&]{
                static_assert(sizeof(NumBytesType) >= sizeof(SizeOfHeaderType));
                if (isFirstThread) {
                    return (NumBytesType) ReadBytesStartingAt<SizeOfHeaderType>(&constants.outputBuffer[SizeOfHeaderOffset]);
                } else {
                    return ReadBytesStartingAt<NumBytesType>(&constants.outputBuffer[NumBytesOffset]);
                }
            }();
#endif

            size_t nextAvailableSlot = startIndex;

#ifndef __METAL__
            std::cout << "Next available slot: " << nextAvailableSlot << std::endl;
#endif
            // Write the column sizes for all non-zero size columns
            for (size_t i = 0; i < row.NumColumns(); ++i) {
                if (row.ColumnVariableSize(i)) {
                    auto rowSize = row.ColumnSize(i);
                    WriteBytesStartingAt(&constants.outputBuffer[nextAvailableSlot], rowSize);
                    nextAvailableSlot += sizeof(rowSize);
                }
            }

            // Write the data for the row
            for (size_t i = 0; i < row.size(); ++i) {
                const auto value = row.data()[i];
#ifndef __METAL__
//                std::cout << "Writing at position: " << nextAvailableSlot << std::endl;
#endif
                constants.outputBuffer[nextAvailableSlot++] = value;
            }

#ifndef __METAL__
            // Update the size of the buffer
            std::cout << "Writing Num Bytes Final at: " << NumBytesOffset << " with value: " << nextAvailableSlot << std::endl;
            WriteBytesStartingAt<NumBytesType>(&constants.outputBuffer[NumBytesOffset], nextAvailableSlot);
#endif
        }

    private:
        InstSerializedValuePtr _instructions;
    };
}
