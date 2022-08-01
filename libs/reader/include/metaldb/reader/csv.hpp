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

        bool IsValid() const noexcept;

        RawTable Read(const CSVOptions& options) const noexcept;
    private:
        std::filesystem::path _path;
    };
}
