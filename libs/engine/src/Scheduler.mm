#include "Scheduler.hpp"

std::vector<char> metaldb::Scheduler::SerializeRawTable(const metaldb::reader::RawTable& rawTable) {
    std::vector<char> rawDataSerialized;

    {
        // Write header section
        // size of the header
        using HeaderSizeType = decltype(((metaldb::RawTable*)nullptr)->GetSizeOfHeader());
        constexpr auto sizeOfHeaderType = sizeof(HeaderSizeType);
        constexpr auto sizeOfSizeType = sizeof(metaldb::types::SizeType);

        using NumRowsType = decltype(((metaldb::RawTable*)nullptr)->GetNumRows());
        constexpr auto sizeOfNumRowsType = sizeof(NumRowsType);

        using RowIndexType = decltype(((metaldb::RawTable*)nullptr)->GetRowIndex(0));
        constexpr auto sizeOfRowIndexType = sizeof(RowIndexType);

        HeaderSizeType sizeOfHeader = 0;

        // Allocate space for header size
        for (std::size_t i = 0; i < sizeOfHeaderType; ++i) {
            rawDataSerialized.push_back(0);
        }
        sizeOfHeader += sizeOfHeaderType;

        // Size of data
        // Allocate space for buffer size
        {
            const metaldb::types::SizeType size = rawTable.data.size();
            for (std::size_t n = 0; n < sizeOfSizeType; ++n) {
                // Read the nth byte
                rawDataSerialized.push_back((int8_t)(size >> (8 * n)) & 0xff);
            }
            sizeOfHeader += sizeOfSizeType;
        }

        // num rows
        {
            const NumRowsType num = rawTable.numRows();
            for (std::size_t n = 0; n < sizeOfNumRowsType; ++n) {
                // Read the nth byte
                rawDataSerialized.push_back((int8_t)(num >> (8 * n)) & 0xff);
            }
            sizeOfHeader += sizeOfNumRowsType;
        }

        // Write the row indexes
        {
            for (const auto& index : rawTable.rowIndexes) {
                RowIndexType num = index;
                for (std::size_t n = 0; n < sizeOfRowIndexType; ++n) {
                    // Read the nth byte
                    rawDataSerialized.push_back((int8_t)(num >> (8 * n)) & 0xff);
                }

                sizeOfHeader += sizeOfRowIndexType;
            }
        }

        // Write in size of header
        for (std::size_t n = 0; n < sizeOfHeaderType; ++n) {
            // Read the nth byte
            rawDataSerialized.at(n) = ((int8_t)(sizeOfHeader >> (8 * n)) & 0xff);
        }
    }

    std::copy(rawTable.data.begin(), rawTable.data.end(), std::back_inserter(rawDataSerialized));
    return rawDataSerialized;
}
