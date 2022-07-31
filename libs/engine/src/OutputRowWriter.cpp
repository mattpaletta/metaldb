#include "OutputRowWriter.hpp"

metaldb::OutputRowWriter::OutputRowWriter(const OutputRowBuilder& builder) {
    this->_sizeOfHeader = OutputRow::ColumnTypeOffset;

    this->_columnTypes = builder.columnTypes;
    this->_sizeOfHeader += sizeof(ColumnType) * this->NumColumns();
    this->_hasCopiedHeader = true;
}

auto metaldb::OutputRowWriter::CurrentNumRows() const -> OutputRow::NumRowsType {
    return this->_numRows;
}

auto metaldb::OutputRowWriter::size() const -> std::size_t {
    return
        sizeof(this->_sizeOfHeader) + // Size of header size.
        this->_sizeOfHeader +
        this->_data.size();
}

void metaldb::OutputRowWriter::appendTempRow(const metaldb::TempRow& row) {
    if (!this->_hasCopiedHeader) {
        this->_sizeOfHeader = OutputRow::ColumnTypeOffset;

        for (std::size_t col = 0; col < row.NumColumns(); ++col) {
            this->_columnTypes.push_back(row.ColumnType(col));
        }
        this->_sizeOfHeader += sizeof(ColumnType) * this->NumColumns();
        this->_hasCopiedHeader = true;
    }
    assert(row.NumColumns() == this->NumColumns());

    // Copy the variable row sizes and the data
    for (std::size_t col = 0; col < row.NumColumns(); ++col) {
        if (row.ColumnVariableSize(col)) {
            // Write the size of it.
            this->appendToData(row.ColumnSize(col));
        }
    }
    for (std::size_t i = 0; i < row.size(); ++i) {
        // Write the bytes to it.
        this->appendToData(*row.data(i));
    }
    this->_numRows++;
}

auto metaldb::OutputRowWriter::SizeOfHeader() const -> OutputRow::SizeOfHeaderType {
    return OutputRow::SizeOfHeader(this->NumColumns());
}

void metaldb::OutputRowWriter::write(std::vector<char>& buffer) const {
    // Write the header
    const auto sizeOfHeader = this->SizeOfHeader();

    // Insert padding
    this->addPaddingUntilIndex(OutputRow::SizeOfHeaderOffset, buffer);
    this->appendGeneric(sizeOfHeader, buffer);

    this->addPaddingUntilIndex(OutputRow::NumBytesOffset, buffer);
    this->appendGeneric(this->NumBytes(), buffer);

    this->addPaddingUntilIndex(OutputRow::NumColumnsOffset, buffer);
    this->appendGeneric(this->NumColumns(), buffer);

    this->addPaddingUntilIndex(OutputRow::ColumnTypeOffset, buffer);
    for (const auto& colType : this->_columnTypes) {
        this->appendGeneric(colType, buffer);
    }
    std::copy(this->_data.cbegin(), this->_data.cend(), std::back_inserter(buffer));
}

auto metaldb::OutputRowWriter::NumColumns() const -> OutputRow::NumColumnsType {
    return (OutputRow::NumColumnsType) this->_columnTypes.size();
}

auto metaldb::OutputRowWriter::NumBytes() const -> OutputRow::NumBytesType {
    // NumBytes should include the size of the header.
    return this->NumBytesData() + this->SizeOfHeader();
}

auto metaldb::OutputRowWriter::NumBytesData() const -> OutputRow::NumBytesType {
    // This is only the size of the data, without the header.
    return (OutputRow::NumBytesType) this->_data.size();
}

void metaldb::OutputRowWriter::addPaddingUntilIndex(size_t index, std::vector<char>& buffer) const {
    // Subract 1 for the last index, subtract 1 for the next index.
    while (index > 0 && buffer.size() < index) {
        this->appendGeneric((uint8_t) 0, buffer);
    }
}
