//
//  RawTable.cpp
//  metaldb_reader
//
//  Created by Matthew Paletta on 2022-03-05.
//

#include <metaldb/reader/RawTable.hpp>
#include <sstream>
#include <algorithm>

metaldb::reader::RawTable::RawTable(bool isValid) : data(), rowIndexes(), columns(), _isValid(isValid) {}

metaldb::reader::RawTable::RawTable(std::vector<char>&& buffer, const std::vector<std::uint16_t>& rowIndexes, const std::vector<std::string>& columns) : data(std::move(buffer)), rowIndexes(rowIndexes), columns(columns), _isValid(true) {}

auto metaldb::reader::RawTable::placeholder() -> std::shared_ptr<RawTable> {
    return std::make_shared<RawTable>(RawTable::invalid());
}

auto metaldb::reader::RawTable::invalid() -> RawTable {
    return RawTable(false);
}

auto metaldb::reader::RawTable::isValid() const -> bool {
    return this->_isValid;
}

auto metaldb::reader::RawTable::numRows() const -> std::uint32_t {
    return this->rowIndexes.size();
}

auto metaldb::reader::RawTable::readRow(std::size_t row) const -> std::string {
    if (row >= this->numRows()) {
        return "";
    }

    std::size_t beginningIndex = this->rowIndexes.at(row);
    std::size_t endIndex = (row == this->numRows() - 1) ? this->data.size() : this->rowIndexes.at(row + 1);

    std::stringstream sstream;
    for (std::size_t i = beginningIndex; i < endIndex; ++i) {
        sstream << this->data.at(i);
    }

    return sstream.str();
}

auto metaldb::reader::RawTable::debugStr() const -> std::string {
    std::stringstream sstream;
    sstream << "************************\n";
    sstream << "Num Columns: " << this->columns.size() << "\n";
    sstream << "Num Rows: " << this->numRows() << "\n";
    sstream << "Num Bytes: " << this->data.size() << "\n";
    sstream << "Columns\n";
    for (const auto& col : this->columns) {
        sstream << col << " ";
    }
    sstream << "\n";
    sstream << "First " << std::min(100UL, this->data.size()) << " bytes:\n";
    for (std::size_t i = 0; i < std::min(100UL, this->data.size()); ++i) {
        sstream << this->data.at(i);
    }
    sstream << "\n";
    sstream << "First " << std::min(3u, this->numRows()) << " rows:\n";
    for (std::size_t i = 0; i < std::min(3u, this->numRows()); ++i) {
        const auto row = this->readRow(i);
        sstream << row << "\n";
    }
    sstream << "************************\n";

    return sstream.str();
}
