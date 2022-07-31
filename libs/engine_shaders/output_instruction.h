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

            const auto index = constants.thread_position_in_threadgroup;
            const auto isFirstThread = index == 0;
#ifdef __METAL__
            // Sync here
            threadgroup_barrier(metal::mem_flags::mem_threadgroup);

            NumBytesType rowSize = row.SizeOfPartialRow();
            if (isFirstThread) {
                rowSize += OutputRow::SizeOfHeader(row.NumColumns());
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
                SizeOfHeaderType lengthOfHeader = 0;

#ifndef __METAL__
                // Placeholder
                NumBytesType bufferSize = 0;
#endif
                // Write length of buffer
                WriteBytesStartingAt(&constants.outputBuffer[NumBytesOffset], bufferSize);

                // Write the number of columns
                {
                    auto numColumns = row.NumColumns();
                    WriteBytesStartingAt(&constants.outputBuffer[NumColumnsOffset], numColumns);
                }

                // Compute length of header here because we know everything above is constant space.
                lengthOfHeader = ColumnTypeOffset;

                // Write the types of each column
                for (auto i = 0; i < row.NumColumns(); ++i) {
                    auto columnType = row.ColumnType(i);
                    WriteBytesStartingAt(&constants.outputBuffer[lengthOfHeader], columnType);
                    lengthOfHeader += sizeof(columnType);
                }

                // Write the size of the header
                WriteBytesStartingAt(&constants.outputBuffer[SizeOfHeaderOffset], lengthOfHeader);
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
                constants.outputBuffer[nextAvailableSlot++] = value;
            }

#ifndef __METAL__
            // Update the size of the buffer
            assert(nextAvailableSlot - startIndex == row.SizeOfPartialRow());
            WriteBytesStartingAt<NumBytesType>(&constants.outputBuffer[NumBytesOffset], nextAvailableSlot);
#endif
        }

    private:
        InstSerializedValuePtr _instructions;
    };
}
