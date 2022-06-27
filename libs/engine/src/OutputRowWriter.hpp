//
//  OutputRowWriter.hpp
//  metaldb_engine
//
//  Created by Matthew Paletta on 2022-05-29.
//

#pragma once

#include "OutputRowReader.hpp"
#include "output_row.h"
#include "temp_row.h"

#include <vector>

namespace metaldb {
    class OutputRowWriter final {
    public:
        class OutputRowBuilder {
        public:
            std::vector<ColumnType> columnTypes;
        };

        OutputRowWriter() = default;
        OutputRowWriter(const OutputRowBuilder& builder) {
            this->_sizeOfHeader = OutputRow::ColumnTypeOffset;

            this->_columnTypes = builder.columnTypes;
            this->_sizeOfHeader += sizeof(ColumnType) * this->NumColumns();
            this->_hasCopiedHeader = true;
        }

        ~OutputRowWriter() = default;

        OutputRow::NumRowsType CurrentNumRows() const {
            return this->_numRows;
        }

        size_t size() const {
            return
                sizeof(this->_sizeOfHeader) + // Size of header size.
                this->_sizeOfHeader +
                this->_data.size();
        }

        void appendTempRow(const metaldb::TempRow& row) {
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

        template<typename Container>
        void copyRow(const OutputRowReader<Container>& reader, std::size_t row) {
            if (!this->_hasCopiedHeader) {
                // The num bytes also includes the size of the header, but that's left to when we retreive.
                this->_sizeOfHeader = OutputRow::ColumnTypeOffset;

                // Write types of columns
                this->_columnTypes = reader.ColumnTypes();
                this->_sizeOfHeader += sizeof(ColumnType) * this->NumColumns();
                assert(this->NumColumns() == reader.NumColumns());
                this->_hasCopiedHeader = true;
            }

            for (const auto& c : reader.VariableLengthColumns()) {
                // Write in the variable length colunns for the row.
                this->appendToData(reader.SizeOfColumn(c, row));
            }

            for (auto c = 0; c < reader.NumColumns(); ++c) {
                auto [columnStart, columnSize] = reader.ColumnIndexInfo(c, row);
                const auto columnEnd = columnStart + columnSize;
                for (auto i = columnStart; i < columnEnd; ++i) {
                    this->appendToData(reader.Raw().at(i));
                }
            }
            this->_numRows++;
        }

        OutputRow::SizeOfHeaderType SizeOfHeader() const {
            constexpr decltype(this->_sizeOfHeader) sizeOfHeaderBase = OutputRow::ColumnTypeOffset;
            return sizeOfHeaderBase + (sizeof(ColumnType) * this->NumColumns());
        }

        void write(std::vector<char>& buffer) {
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

        OutputRow::NumColumnsType NumColumns() const {
            return (OutputRow::NumColumnsType) this->_columnTypes.size();
        }

        OutputRow::NumBytesType NumBytes() const {
            // NumBytes should include the size of the header.
            return this->NumBytesData() + this->SizeOfHeader();
        }

        OutputRow::NumBytesType NumBytesData() const {
            // This is only the size of the data, without the header.
            return (OutputRow::NumBytesType) this->_data.size();
        }

    private:
        bool _hasCopiedHeader = false;
        OutputRow::SizeOfHeaderType _sizeOfHeader = 0;
        OutputRow::NumRowsType _numRows = 0;
        std::vector<ColumnType> _columnTypes;

        std::vector<char> _data;

        void addPaddingUntilIndex(size_t index, std::vector<char>& buffer) {
            // Subract 1 for the last index, subtract 1 for the next index.
            while (index > 0 && buffer.size() < index) {
                this->appendGeneric((uint8_t) 0, buffer);
            }
        }

        template<typename T>
        void appendToData(T val) {
            static_assert(sizeof(decltype(_data)::value_type) == 1);
            return this->appendGeneric(val, this->_data);
        }

        template<typename T, typename V>
        void appendGeneric(T val, std::vector<V>& buf) {
            WriteBytesStartingAt(buf, val);
        }
    };
}
