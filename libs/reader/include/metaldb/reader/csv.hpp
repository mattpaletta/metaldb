#pragma once

#include "RawTable.hpp"

#include <string>
#include <filesystem>

namespace metaldb::reader {
    class CSVReader {
    public:
        struct CSVOptions {
            bool containsHeaderLine = false;
            bool stripQuotesFromHeader = false;
        };

        CSVReader(const std::filesystem::path& path);

        bool isValid() const;

        RawTable read(const CSVOptions& options) const;
    private:
        std::filesystem::path _path;
    };
}
