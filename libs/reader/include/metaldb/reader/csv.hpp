#pragma once

#include "RawTable.hpp"

#include <filesystem>

namespace metaldb::reader {
    class CSVReader {
    public:
        struct CSVOptions {
            bool containsHeaderLine = false;
            bool stripQuotesFromHeader = false;
        };

        CSVReader(std::filesystem::path path) noexcept;

        /**
         * Returns true if the CSV Reader was given a valid CSV file.
         */
        bool IsValid() const noexcept;

        /**
         * Reads a CSV file into a buffer and returns it as a RawTable.
         * @param options An options config describing how to read the CSV file.
         *
         * If the CSV cannot be read, a `RawTable::invalid` object will be returned.
         */
        RawTable Read(const CSVOptions& options) const noexcept;
    private:
        std::filesystem::path _path;
    };
}
