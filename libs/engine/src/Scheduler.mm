#include "Scheduler.hpp"
#include <algorithm>

auto metaldb::Scheduler::SerializeRawTable(const metaldb::reader::RawTable& rawTable, std::size_t maxChunkSize) -> std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> {
    using SizeOfHeaderType = RawTable::SizeOfHeaderType;
    using SizeOfDataType = RawTable::SizeOfDataType;
    using NumRowsType = RawTable::NumRowsType;
    using RowIndexType = RawTable::RowIndexType;

    std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> output;
    auto rawDataSerialized = MakeBufferPtr();

    std::size_t lastDataOffset = 0;
    auto prepareChunk = [&](NumRowsType startRow, NumRowsType endRow) {
        const auto numRowsLocal = endRow - startRow;
        if (numRowsLocal == 0 || endRow < startRow /* if overflowed */) {
            return;
        }

        // Write header section

        // size of the header
        SizeOfHeaderType sizeOfHeader = 0;
        {
            // Allocate space for header size
            for (std::size_t i = 0; i < sizeof(SizeOfHeaderType); ++i) {
                rawDataSerialized->push_back(0);
            }
            sizeOfHeader += sizeof(SizeOfHeaderType);
        }


        // Allocate space for buffer size
        SizeOfDataType numBytes = 0;

        // Size of data
        {
#ifdef DEBUG
            assert(rawDataSerialized->size() == RawTable::SizeOfHeaderOffset);
#endif
            for (auto n = 0; n < sizeof(SizeOfDataType); ++n) {
                // Read the nth byte
                rawDataSerialized->push_back(0);
            }
            sizeOfHeader += sizeof(SizeOfDataType);
        }

        // num rows
        {
            const NumRowsType num = numRowsLocal;
#ifdef DEBUG
            assert(rawDataSerialized->size() == RawTable::NumRowsOffset);
#endif
            for (std::size_t n = 0; n < sizeof(NumRowsType); ++n) {
                // Read the nth byte
                rawDataSerialized->push_back((int8_t)(num >> (8 * n)) & 0xff);
            }
            sizeOfHeader += sizeof(NumRowsType);
        }

        // Write the row indexes
        {
#ifdef DEBUG
            assert(rawDataSerialized->size() == RawTable::RowIndexOffset);
#endif
            for (auto row = startRow; row < endRow; ++row) {
                // Last data access stores the highest index on the last iteration, which we offset
                // because those rows are not in this batch.
                RowIndexType index = rawTable.rowIndexes.at(row) - lastDataOffset;
                for (std::size_t n = 0; n < sizeof(RowIndexType); ++n) {
                    // Read the nth byte
                    rawDataSerialized->push_back((int8_t)(index >> (8 * n)) & 0xff);
                }
            }
            sizeOfHeader += (sizeof(RowIndexType) * numRowsLocal);
        }

        {
            // Write in the data
            for (auto row = startRow; row < endRow; ++row) {
                RowIndexType index = rawTable.rowIndexes.at(row);
                if (row == rawTable.rowIndexes.size() - 1) {
                    // This is the last row, read until the end.
                    for (std::size_t i = index; i < rawTable.data.size(); ++i) {
                        rawDataSerialized->push_back(rawTable.data.at(i));
                    }
                    numBytes += (rawTable.data.size() - index);
                    lastDataOffset = std::max(lastDataOffset, rawTable.data.size() - 1);
                } else {
                    // Read until the next row
                    RowIndexType nextRow = rawTable.rowIndexes.at(row + 1);
                    for (auto i = index; i < nextRow; ++i) {
                        rawDataSerialized->push_back(rawTable.data.at(i));
                    }
                    numBytes += (nextRow - index);
                    lastDataOffset = std::max(lastDataOffset, nextRow - 1UL);
                }
            }
        }

        // Final cleanup
        WriteBytesStartingAt(&rawDataSerialized->at(RawTable::SizeOfDataOffset), numBytes);
        WriteBytesStartingAt(&rawDataSerialized->at(RawTable::SizeOfHeaderOffset), sizeOfHeader);

        // Flush buffer
        output.emplace_back(rawDataSerialized, numRowsLocal);
        rawDataSerialized = MakeBufferPtr();
    };

    // Chunk it into the max size appropriately.
    NumRowsType i = 0;
    if (rawTable.numRows() > maxChunkSize) {
        for (; i < rawTable.numRows(); i += maxChunkSize) {
            prepareChunk(i, i + maxChunkSize);
        }
    }
    prepareChunk(i, rawTable.numRows());

    return output;
}
