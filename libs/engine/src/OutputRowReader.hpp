//
//  OutputRowReader.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-05-21.
//

#pragma once

#include "column_type.h"
#include "output_row.h"

#include <vector>
#include <iostream>

namespace metaldb {
    template<typename Container = std::vector<char>>
    class OutputRowReader {
    public:
        using value_type = typename Container::value_type;

        OutputRowReader(const Container& instructions) : _instructions(instructions) {
            this->_sizeOfHeader = *(decltype(_sizeOfHeader) *)(&instructions.at(OutputRow::SizeOfHeaderOffset));
            this->_numBytes = *(decltype(_numBytes) *)(&instructions.at(OutputRow::NumBytesOffset));

            this->_numColumns = *(decltype(_numColumns) *)(&instructions.at(OutputRow::NumColumnsOffset));

            this->_columnSizes.resize(this->_numColumns);
            this->_columnTypes.resize(this->_numColumns);

            for (size_t i = 0; i < this->_numColumns; ++i) {
                const auto columnType = (ColumnType) instructions.at(OutputRow::ColumnTypeOffset + i);
                this->_columnTypes.at(i) = columnType;

                switch (columnType) {
                case String:
                    this->_columnSizes.at(i) = 0;
                    this->_variableLengthColumns.push_back(i);
                    break;
                case Float:
                    this->_columnSizes.at(i) = sizeof(metaldb::types::FloatType);
                    break;
                case Integer:
                    this->_columnSizes.at(i) = sizeof(metaldb::types::IntegerType);
                    break;
                case Unknown:
                    assert(false);
                    break;
                }
            }

            std::size_t i = this->_sizeOfHeader;
            std::size_t rowNum = 0;
            while (i < this->_numBytes + this->_sizeOfHeader) {
                if (rowNum % 10000 == 0) {
                    std::cout << "Reading row: " << rowNum++ << "\n";
                }
                this->_rowStartOffset.push_back(i);

                // Read the column sizes for the dynamic sized ones
                auto columnSizes = this->_columnSizes;
                for (const auto& varLengthCol : this->_variableLengthColumns) {
                    columnSizes.at(varLengthCol) = (OutputRow::ColumnSizeType) instructions.at(i++);
                }

                // Read the row
                for (int col = 0; col < this->_numColumns; ++col) {
                    auto columnSize = columnSizes.at(col);
                    i += columnSize;
                }
            }
        }

        OutputRow::ColumnSizeType SizeOfColumn(size_t column, size_t row) const {
            return this->ColumnSizesForRow(row).at(column);
        }

        ColumnType TypeOfColumn(size_t column) const {
            return this->_columnTypes.at(column);
        }

        std::vector<ColumnType> ColumnTypes() const {
            return this->_columnTypes;
        }

        std::size_t StartOfColumn(size_t column, size_t row) const {
            auto columnSizes = this->ColumnSizesForRow(row);
            return this->StartOfColumn(column, row, columnSizes);
        }

        std::pair<std::size_t, OutputRow::ColumnSizeType> ColumnIndexInfo(size_t column, size_t row) const {
            auto columnSizes = this->ColumnSizesForRow(row);
            return std::make_pair(this->StartOfColumn(column, row, columnSizes), columnSizes.at(column));
        }

        OutputRow::SizeOfHeaderType SizeOfHeader() const {
            return this->_sizeOfHeader;
        }

        OutputRow::NumBytesType NumBytes() const {
            return this->_numBytes;
        }

        OutputRow::NumColumnsType NumColumns() const {
            return this->_numColumns;
        }

        std::uint32_t NumRows() const {
            return (std::uint32_t) this->_rowStartOffset.size();
        }

        const Container& Raw() const {
            return this->_instructions;
        }

    private:
        const Container& _instructions;
        OutputRow::SizeOfHeaderType _sizeOfHeader = 0;
        OutputRow::NumBytesType _numBytes = 0;
        OutputRow::NumColumnsType _numColumns = 0;

        std::vector<OutputRow::ColumnSizeType> _columnSizes;
        std::vector<ColumnType> _columnTypes;
        std::vector<size_t> _variableLengthColumns;
        std::vector<size_t> _rowStartOffset;
        
        std::size_t StartOfColumn(size_t column, size_t row, const decltype(_columnSizes) columnSizes) const {
            size_t offset = this->_rowStartOffset.at(row);
            for (size_t i = 0; i < column; ++i) {
                offset += columnSizes.at(i);
            }
            return offset;
        }

        decltype(_columnSizes) ColumnSizesForRow(size_t row) const {
            auto rowStart = this->_rowStartOffset.at(row);

            auto columnSizes = this->_columnSizes;
            for (const auto& varLengthCol : this->_variableLengthColumns) {
                columnSizes.at(varLengthCol) = (OutputRow::ColumnSizeType) this->_instructions.at(rowStart++);
            }

            return columnSizes;
        }
    };
}
