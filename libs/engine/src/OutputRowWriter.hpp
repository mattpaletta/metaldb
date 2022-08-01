#pragma once

#include "OutputRowReader.hpp"
#include "output_row.h"
#include "temp_row.h"

#include <vector>
#include <cassert>

namespace metaldb {
    class OutputRowWriter final {
    public:
        class OutputRowBuilder {
        public:
            std::vector<ColumnType> columnTypes;
        };

        OutputRowWriter() = default;
        OutputRowWriter(const OutputRowBuilder& builder);

        ~OutputRowWriter() noexcept = default;

        CPP_PURE_FUNC OutputRow::NumRowsType CurrentNumRows() const noexcept;

        CPP_PURE_FUNC size_t size() const noexcept;

        void appendTempRow(const metaldb::TempRow& row) noexcept;

        template<typename Container>
        void copyRow(const OutputRowReader<Container>& reader, std::size_t row) noexcept {
            if (!this->_hasCopiedHeader) {
                // The num bytes also includes the size of the header, but that's left to when we retreive.
                this->_sizeOfHeader = OutputRow::SizeOfHeader(this->NumColumns());

                // Write types of columns
                this->_columnTypes = reader.ColumnTypes();
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

        CPP_PURE_FUNC OutputRow::SizeOfHeaderType SizeOfHeader() const noexcept;

        CPP_PURE_FUNC void write(std::vector<char>& buffer) const noexcept;

        CPP_PURE_FUNC OutputRow::NumColumnsType NumColumns() const noexcept;

        CPP_PURE_FUNC OutputRow::NumBytesType NumBytes() const noexcept;

        CPP_PURE_FUNC OutputRow::NumBytesType NumBytesData() const noexcept;

    private:
        bool _hasCopiedHeader = false;
        OutputRow::SizeOfHeaderType _sizeOfHeader = 0;
        OutputRow::NumRowsType _numRows = 0;
        std::vector<ColumnType> _columnTypes;
        std::vector<char> _data;

        void addPaddingUntilIndex(size_t index, std::vector<char>& buffer) const noexcept;

        template<typename T>
        CPP_PURE_FUNC void appendToData(T val) noexcept {
            static_assert(sizeof(decltype(_data)::value_type) == 1);
            return this->appendGeneric(val, this->_data);
        }

        template<typename T, typename V>
        CPP_PURE_FUNC void appendGeneric(T val, std::vector<V>& buf) const noexcept {
            WriteBytesStartingAt(buf, val);
        }
    };
}
