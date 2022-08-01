#include <metaldb/reader/RawTable.hpp>

#include <sstream>
#include <algorithm>

metaldb::reader::RawTable::RawTable(bool isValid) noexcept : _isValid(isValid) {}

metaldb::reader::RawTable::RawTable(std::vector<char> buffer, std::vector<RowIndexType> rowIndexes, std::vector<std::string> columns) noexcept : data(std::move(buffer)), rowIndexes(std::move(rowIndexes)), columns(std::move(columns)), _isValid(true) {}

auto metaldb::reader::RawTable::Placeholder() noexcept -> std::shared_ptr<RawTable> {
    return std::make_shared<RawTable>(RawTable::Invalid());
}

auto metaldb::reader::RawTable::Invalid() noexcept -> RawTable {
    return RawTable(false);
}

auto metaldb::reader::RawTable::IsValid() const noexcept -> bool {
    return this->_isValid;
}

auto metaldb::reader::RawTable::NumRows() const noexcept -> std::uint32_t {
    return this->rowIndexes.size();
}

auto metaldb::reader::RawTable::NumColumns() const noexcept -> std::uint32_t {
    return this->columns.size();
}

auto metaldb::reader::RawTable::NumBytes() const noexcept -> std::uint32_t {
    return this->data.size();
}

auto metaldb::reader::RawTable::ReadRow(std::size_t row) const noexcept -> std::string {
    if (row >= this->NumRows()) {
        return "";
    }

    const std::size_t beginningIndex = this->rowIndexes.at(row);
    const std::size_t endIndex = (row == this->NumRows() - 1) ? this->data.size() : this->rowIndexes.at(row + 1);

    std::stringstream sstream;
    for (std::size_t i = beginningIndex; i < endIndex; ++i) {
        sstream << this->data.at(i);
    }

    return sstream.str();
}

auto metaldb::reader::RawTable::DebugStr() const noexcept -> std::string {
    std::stringstream sstream;
    sstream << "************************\n";
    sstream << "Num Columns: " << this->NumColumns() << "\n";
    sstream << "Num Rows: " << this->NumRows() << "\n";
    sstream << "Num Bytes: " << this->NumBytes() << "\n";
    sstream << "Columns\n";
    for (const auto& col : this->columns) {
        sstream << col << " ";
    }
    sstream << "\n";
    sstream << "First " << std::min(100U, this->NumBytes()) << " bytes:\n";
    for (std::size_t i = 0; i < std::min(100U, this->NumBytes()); ++i) {
        sstream << this->data.at(i);
    }
    sstream << "\n";
    sstream << "First " << std::min(3U, this->NumRows()) << " rows:\n";
    for (std::size_t i = 0; i < std::min(3U, this->NumRows()); ++i) {
        const auto row = this->ReadRow(i);
        sstream << row << "\n";
    }
    sstream << "************************\n";

    return sstream.str();
}
