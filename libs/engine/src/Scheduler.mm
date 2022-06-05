#include "Scheduler.hpp"
#include <algorithm>

auto metaldb::Scheduler::SerializeRawTable(const metaldb::reader::RawTable& rawTable, std::size_t maxChunkSize) -> std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> {
    using HeaderSizeType = decltype(((metaldb::RawTable*)nullptr)->GetSizeOfHeader());
    constexpr auto SizeOfHeaderType = sizeof(HeaderSizeType);

    using NumBytesType = decltype(((metaldb::RawTable*)nullptr)->GetSizeOfData());
    constexpr auto SizeOfNumBytesType = sizeof(NumBytesType);

    using NumRowsType = decltype(((metaldb::RawTable*)nullptr)->GetNumRows());
    constexpr auto SizeOfNumRowsType = sizeof(NumRowsType);

    using RowIndexType = decltype(((metaldb::RawTable*)nullptr)->GetRowIndex(0));
    constexpr auto SizeOfRowIndexType = sizeof(RowIndexType);

    std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> output;
    auto rawDataSerialized = MakeBufferPtr();

    std::size_t lastDataOffset = 0;
    auto prepareChunk = [&](std::size_t startRow, std::size_t endRow) {
        const auto numRowsLocal = endRow - startRow;
        if (numRowsLocal == 0) {
            return;
        }

        // Write header section

        // size of the header
        HeaderSizeType sizeOfHeader = 0;
        // Allocate space for header size
        for (std::size_t i = 0; i < SizeOfHeaderType; ++i) {
            rawDataSerialized->push_back(0);
        }
        sizeOfHeader += SizeOfHeaderType;

        NumBytesType numBytes = 0;
        for (std::size_t i = 0; i < SizeOfNumBytesType; ++i) {
            rawDataSerialized->push_back(0);
        }
        sizeOfHeader += SizeOfNumBytesType;

        // num rows
        {
            const NumRowsType num = numRowsLocal;
            for (std::size_t n = 0; n < SizeOfNumRowsType; ++n) {
                // Read the nth byte
                rawDataSerialized->push_back((int8_t)(num >> (8 * n)) & 0xff);
            }
            sizeOfHeader += SizeOfNumRowsType;
        }

        // Write the row indexes
        {
            for (std::size_t row = startRow; row < endRow; ++row) {
                // Last data access stores the highest index on the last iteration, which we offset
                // because those rows are not in this batch.
                RowIndexType index = rawTable.rowIndexes.at(row) - lastDataOffset;
                for (std::size_t n = 0; n < SizeOfRowIndexType; ++n) {
                    // Read the nth byte
                    rawDataSerialized->push_back((int8_t)(index >> (8 * n)) & 0xff);
                }
            }
            sizeOfHeader += (SizeOfRowIndexType * numRowsLocal);
        }
        {
            // Write in the data
            for (std::size_t row = startRow; row < endRow; ++row) {
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
                    for (std::size_t i = index; i < nextRow; ++i) {
                        rawDataSerialized->push_back(rawTable.data.at(i));
                    }
                    numBytes += (nextRow - index);
                    lastDataOffset = std::max(lastDataOffset, nextRow - 1UL);
                }
            }
        }

        // Final cleanup
        {
            // Write in size of buffer
            const NumBytesType size = numBytes;
            for (std::size_t n = 0; n < SizeOfNumBytesType; ++n) {
                // Read the nth byte
                rawDataSerialized->at(SizeOfHeaderType + n) = ((int8_t)(size >> (8 * n)) & 0xff);
            }
        }
        {
            // Write in size of header
            for (std::size_t n = 0; n < SizeOfHeaderType; ++n) {
                // Read the nth byte
                rawDataSerialized->at(n) = ((int8_t)(sizeOfHeader >> (8 * n)) & 0xff);
            }
        }

        // Flush buffer
        output.emplace_back(rawDataSerialized, numRowsLocal);
        rawDataSerialized = MakeBufferPtr();
    };

    std::size_t i = 0;
    if (rawTable.numRows() > maxChunkSize) {
        for (; i < rawTable.numRows(); i += maxChunkSize) {
            prepareChunk(i, i + maxChunkSize);
        }
    }
    prepareChunk(i, /*rawTable.numRows() - 1*/ 10);

    return output;
}
