#pragma once

#include "column_type.h"
#include "output_row.h"

#include <vector>

namespace metaldb {
    template<typename Container = std::vector<char>>
    class OutputRowReader final {
    public:
        using value_type = typename Container::value_type;

        OutputRowReader(const Container& instructions) : _instructions(instructions) {
            this->_sizeOfHeader = OutputRowReader::SizeOfHeader(instructions);
            this->_numBytes = OutputRowReader::NumBytes(instructions);
            this->_numColumns = OutputRowReader::NumColumns(instructions);

            this->_columnSizes.resize(this->_numColumns);
            this->_columnTypes.resize(this->_numColumns);

            for (size_t i = 0; i < this->_numColumns; ++i) {
                const auto index = OutputRow::ColumnTypeOffset + (i * sizeof(ColumnType));
                const auto columnType = ReadBytesStartingAt<ColumnType>(&instructions.at(index));
                this->_columnTypes.at(i) = columnType;

                switch (columnType) {
                case String:
                case String_opt:
                case Float_opt:
                case Integer_opt:
                    this->_columnSizes.at(i) = 0;
                    this->_variableLengthColumns.push_back(i);
                    break;
                case Float:
                case Integer:
                    this->_columnSizes.at(i) = metaldb::BaseColumnSize(columnType);
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

        ~OutputRowReader() noexcept = default;

        CPP_CONST_FUNC std::vector<OutputRow::ColumnSizeType> VariableLengthColumns() const noexcept {
            return this->_variableLengthColumns;
        }

        CPP_CONST_FUNC bool ColumnIsVariableLength(size_t column) const noexcept {
            return this->_variableLengthColumns.find(column) != this->_variableLengthColumns.end();
        }

        CPP_PURE_FUNC OutputRow::ColumnSizeType SizeOfColumn(size_t column, size_t row) const noexcept {
            return this->ColumnSizesForRow(row).at(column);
        }

        CPP_CONST_FUNC ColumnType TypeOfColumn(size_t column) const noexcept {
            return this->_columnTypes.at(column);
        }

        CPP_CONST_FUNC std::vector<ColumnType> ColumnTypes() const noexcept {
            return this->_columnTypes;
        }

        CPP_PURE_FUNC OutputRow::NumBytesType StartOfColumn(size_t column, size_t row) const noexcept {
            const auto columnSizes = this->ColumnSizesForRow(row);
            return this->StartOfColumn(column, row, columnSizes);
        }

        CPP_PURE_FUNC std::pair<OutputRow::NumBytesType, OutputRow::ColumnSizeType> ColumnIndexInfo(size_t column, size_t row) const noexcept {
            auto columnSizes = this->ColumnSizesForRow(row);
            return std::make_pair(this->StartOfColumn(column, row, columnSizes), columnSizes.at(column));
        }

        CPP_CONST_FUNC OutputRow::SizeOfHeaderType SizeOfHeader() const noexcept {
            return this->_sizeOfHeader;
        }

        CPP_CONST_FUNC static OutputRow::SizeOfHeaderType SizeOfHeader(const Container& buffer) noexcept {
            return ReadBytesStartingAt<OutputRow::SizeOfHeaderType>(&buffer.at(OutputRow::SizeOfHeaderOffset));
        }

        CPP_CONST_FUNC OutputRow::NumBytesType NumBytes() const noexcept {
            return this->_numBytes;
        }

        CPP_CONST_FUNC static OutputRow::NumBytesType NumBytes(const Container& buffer) noexcept {
            return ReadBytesStartingAt<OutputRow::NumBytesType>(&buffer.at(OutputRow::NumBytesOffset));
        }

        CPP_CONST_FUNC OutputRow::NumColumnsType NumColumns() const noexcept {
            return this->_numColumns;
        }

        CPP_CONST_FUNC static OutputRow::NumColumnsType NumColumns(const Container& buffer) noexcept {
            return ReadBytesStartingAt<OutputRow::NumColumnsType>(&buffer.at(OutputRow::NumColumnsOffset));
        }

        CPP_CONST_FUNC OutputRow::NumRowsType NumRows() const noexcept {
            return (OutputRow::NumRowsType) this->_rowStartOffset.size();
        }

        const Container& Raw() const noexcept {
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
        
        CPP_CONST_FUNC OutputRow::NumBytesType StartOfColumn(OutputRow::NumColumnsType column, OutputRow::NumRowsType row, const std::vector<OutputRow::ColumnSizeType>& columnSizes) const noexcept {
            auto offset = this->_rowStartOffset.at(row);

            // Skip the variableLength sizes.
            offset += this->_variableLengthColumns.size();

            // Jump N columns.
            for (auto i = 0; i < column; ++i) {
                offset += columnSizes.at(i);
            }
            return offset;
        }

        CPP_PURE_FUNC std::vector<OutputRow::ColumnSizeType> ColumnSizesForRow(OutputRow::NumRowsType row) const noexcept {
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
