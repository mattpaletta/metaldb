#include <metaldb/reader/csv.hpp>

#include <cppnotstdlib/strings.hpp>

#include <fstream>
#include <sstream>

metaldb::reader::CSVReader::CSVReader(const std::filesystem::path& path) : _path(path) {}

auto metaldb::reader::CSVReader::isValid() const -> bool {
    if (this->_path.extension().string() != ".csv") {
        return false;
    }

    if (!std::filesystem::exists(this->_path)) {
        return false;
    }

    if ((std::filesystem::status(this->_path).permissions() & std::filesystem::perms::owner_read) == std::filesystem::perms::none) {
        return false;
    }

    return true;
}

auto metaldb::reader::CSVReader::read(const CSVOptions& options) const -> RawTable {
    std::ifstream myfile(this->_path.string());
    if (!myfile.is_open()) {
        return RawTable::invalid();
    }

    std::size_t charCount = 0;
    std::vector<RawTable::RowIndexType> rowIndex;
    std::vector<char> buffer;
    std::vector<std::string> columns;
    if (options.containsHeaderLine) {
        // Read the first row
        std::stringstream firstRowBuffer;
        while (myfile.good()) {
            // Buffer the row
            char nextChar = myfile.get();
            firstRowBuffer << nextChar;
            if (nextChar == '\n') {
                break;
            }
        }

        // Read out the columns
        {
            std::string columnStr = firstRowBuffer.str();
            if (options.stripQuotesFromHeader) {
                columnStr = cppnotstdlib::replace(columnStr, "\"", "");
                columnStr = cppnotstdlib::replace(columnStr, "'", "");
            }
            auto splitColumns = cppnotstdlib::explode(columnStr, ',');
            columns = std::move(splitColumns);
        }
    }

    // Just read the rows straight
    rowIndex.push_back(charCount);
    while (myfile.good()) {
        char nextChar = myfile.get();
        if (nextChar == '\n') {
            // Don't copy the newline, unneeded space.
            rowIndex.push_back(charCount);
        } else {
            ++charCount;
            buffer.push_back(nextChar);
        }
    }

    return RawTable(std::move(buffer), rowIndex, columns);
}
