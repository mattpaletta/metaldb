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

namespace metaldb {
    template<typename Container = std::vector<char>>
    class OutputRowReader {
    public:
        using value_type = typename Container::value_type;

        OutputRowReader(const Container& instructions) : _instructions(instructions) {
            std::cout << "Reading Header size at index: " << OutputRow::SizeOfHeaderOffset << std::endl;
            this->_sizeOfHeader = ReadBytesStartingAt<OutputRow::SizeOfHeaderType>(&instructions.at(OutputRow::SizeOfHeaderOffset));
            std::cout << "Reading Num bytes at index: " << OutputRow::NumBytesOffset << std::endl;
            this->_numBytes = ReadBytesStartingAt<OutputRow::NumBytesType>(&instructions.at(OutputRow::NumBytesOffset));
            std::cout << "Reading Num columns at index: " << OutputRow::NumColumnsOffset << std::endl;
            this->_numColumns = ReadBytesStartingAt<OutputRow::NumColumnsType>(&instructions.at(OutputRow::NumColumnsOffset));

            this->_columnSizes.resize(this->_numColumns);
            this->_columnTypes.resize(this->_numColumns);

            for (size_t i = 0; i < this->_numColumns; ++i) {
                const auto index = OutputRow::ColumnTypeOffset + (i * sizeof(ColumnType));
                std::cout << "Reading Column Type at index: " << index << std::endl;
                const auto columnType = ReadBytesStartingAt<ColumnType>(&instructions.at(index));
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
            while (i < this->_numBytes) {
                this->_rowStartOffset.push_back(i);

                // Read the column sizes for the dynamic sized ones
                auto columnSizes = this->_columnSizes;
                for (const auto& varLengthCol : this->_variableLengthColumns) {
                    columnSizes.at(varLengthCol) = ReadBytesStartingAt<OutputRow::ColumnSizeType>(&instructions.at(i));
                    i += sizeof(OutputRow::ColumnSizeType);
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

        OutputRow::NumBytesType StartOfColumn(size_t column, size_t row) const {
            auto columnSizes = this->ColumnSizesForRow(row);
            return this->StartOfColumn(column, row, columnSizes);
        }

        std::pair<OutputRow::NumBytesType, OutputRow::ColumnSizeType> ColumnIndexInfo(size_t column, size_t row) const {
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

        OutputRow::NumRowsType NumRows() const {
            return (OutputRow::NumRowsType) this->_rowStartOffset.size();
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
        std::vector<OutputRow::ColumnSizeType> _variableLengthColumns;
        std::vector<OutputRow::NumBytesType> _rowStartOffset;
        
        OutputRow::NumBytesType StartOfColumn(OutputRow::NumColumnsType column, OutputRow::NumRowsType row, const std::vector<OutputRow::ColumnSizeType>& columnSizes) const {
            auto offset = this->_rowStartOffset.at(row);
            for (auto i = 0; i < column; ++i) {
                offset += columnSizes.at(i);
            }
            return offset;
        }

        std::vector<OutputRow::ColumnSizeType> ColumnSizesForRow(OutputRow::NumRowsType row) const {
            auto rowStart = this->_rowStartOffset.at(row);

            auto columnSizes = this->_columnSizes;
            for (const auto& varLengthCol : this->_variableLengthColumns) {
                columnSizes.at(varLengthCol) = ReadBytesStartingAt<OutputRow::ColumnSizeType>(&this->_instructions.at(rowStart));
                rowStart += sizeof(OutputRow::ColumnSizeType);
            }

            return columnSizes;
        }
    };
}
