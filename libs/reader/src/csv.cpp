#include <metaldb/reader/csv.hpp>

#include <cppnotstdlib/strings.hpp>

#include <fstream>
#include <sstream>

metaldb::reader::CSVReader::CSVReader(std::filesystem::path path) : _path(std::move(path)) {}

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
            char nextChar = static_cast<char>(myfile.get());
            if (nextChar != '\r' && nextChar != '\n') {
                firstRowBuffer << nextChar;
            }
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
    bool lastWasNewLine = false;
    while (myfile.good()) {
        const auto nextChar = static_cast<char>(myfile.get());
        if (nextChar == '\n' || nextChar == '\r') {
            // Don't copy the newline, unneeded space.
            // Ignore consecutive empty lines
            if (!lastWasNewLine) {
                rowIndex.push_back(charCount);
                lastWasNewLine = true;
            }
        } else if (nextChar != EOF) {
            ++charCount;
            buffer.push_back(nextChar);
            lastWasNewLine = false;
        }
    }
    
    if (lastWasNewLine) {
        // The last newLine should not be included.
        rowIndex.pop_back();
    }

    return RawTable(std::move(buffer), rowIndex, columns);
}
