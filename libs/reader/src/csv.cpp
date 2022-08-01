#include <metaldb/reader/csv.hpp>

#include <cppnotstdlib/strings.hpp>

#include <fstream>
#include <sstream>

metaldb::reader::CSVReader::CSVReader(std::filesystem::path path) noexcept : _path(std::move(path)) {}

auto metaldb::reader::CSVReader::IsValid() const noexcept -> bool {
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

auto metaldb::reader::CSVReader::Read(const CSVOptions& options) const noexcept -> RawTable {
    std::ifstream myfile(this->_path.string());
    if (!myfile.is_open()) {
        return RawTable::Invalid();
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
            const auto nextChar = static_cast<char>(myfile.get());
            if (nextChar != '\r' && nextChar != '\n') {
                firstRowBuffer << nextChar;
            }
            if (nextChar == '\n') {
                break;
            }
        }

        // Read out the columns
        {
            const auto columnStr = [&]{
                auto str = firstRowBuffer.str();
                if (options.stripQuotesFromHeader) {
                    str = cppnotstdlib::replace(str, "\"", "");
                    return cppnotstdlib::replace(str, "'", "");
                }

                return str;
            }();
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
