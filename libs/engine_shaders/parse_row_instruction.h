//
//  parse_row_instruction.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "constants.h"
#include "column_type.h"
#include "method.h"
#include "string_section.h"
#include "temp_row.h"
#include "raw_table.h"
#include "db_constants.h"
#include "strings.h"

namespace metaldb {
    class ParseRowInstruction {
    public:
        using MethodType = Method;
        METAL_CONSTANT static constexpr auto MethodOffset = 0;

        using SkipHeaderType = bool;
        METAL_CONSTANT static constexpr auto SkipHeaderOffset = sizeof(MethodType) + MethodOffset;

        using NumColumnsType = OutputRow::NumColumnsType;
        METAL_CONSTANT static constexpr auto NumColumnsOffset = sizeof(SkipHeaderType) + SkipHeaderOffset;

        METAL_CONSTANT static constexpr auto ColumnTypeOffset = sizeof(NumColumnsType) + NumColumnsOffset;

        using ColumnSizeType = TempRow::ColumnSizeType;
        using RowNumType = uint16_t;

        // Pointer points to beginning of ParseRow instruction.
#ifndef __METAL__
        ParseRowInstruction() : _instructions(nullptr) {}
#endif
        ParseRowInstruction(InstSerializedValuePtr instructions) : _instructions(instructions) {}

        MethodType getMethod() const {
            return ReadBytesStartingAt<MethodType>(&this->_instructions[MethodOffset]);
        }

        SkipHeaderType skipHeader() const {
            return ReadBytesStartingAt<SkipHeaderType>(&this->_instructions[SkipHeaderOffset]);
        }

        NumColumnsType numColumns() const {
            return ReadBytesStartingAt<NumColumnsType>(&this->_instructions[NumColumnsOffset]);
        }

        ColumnType getColumnType(NumColumnsType index) const {
            return ReadBytesStartingAt<ColumnType>(&this->_instructions[(index * sizeof(ColumnType)) + ColumnTypeOffset]);
        }

        InstSerializedValuePtr end() const {
            // Returns 1 past the end of the instruction
            const auto numColumns = this->numColumns();
            const auto offset = ColumnTypeOffset + (numColumns * sizeof(ColumnType));
            return &this->_instructions[offset];
        }

        StringSection readCSVColumn(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const {
            return this->readCSVColumnImpl(rawTable, row, column);
        }

        ColumnSizeType readCSVColumnLength(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const {
            // Do it this way for now to keep the code the same.
            return this->readCSVColumn(rawTable, row, column).size();
        }

        TempRow GetRow(DbConstants METAL_THREAD & constants) {
            TempRow::TempRowBuilder builder;
            auto numCols = this->numColumns();
            {
                builder.numColumns = numCols;

                // Read column types
                for (auto i = 0; i < numCols; ++i) {
                    // Set all column types
                    auto columnType = this->getColumnType(i);
                    builder.columnTypes[i] = columnType;

                    // Set all column sizes, and they might get pruned
                    if (ColumnVariableSize(columnType)) {
                        builder.columnSizes[i] = this->readCSVColumnLength(constants.rawTable, constants.thread_position_in_grid, i);
                    } else {
                        builder.columnSizes[i] = 0;
                    }
                }
            }

            // Populate the row.
            TempRow row = builder;
            for (auto i = 0; i < numCols; ++i) {
                // Write the columns into the buffer
                auto stringSection = this->readCSVColumn(constants.rawTable, constants.thread_position_in_grid, i);

                switch (this->getColumnType(i)) {
                case String:
                case String_opt: {
                    // Copy the string in directly.
                    row.append(stringSection.c_str(), stringSection.size());
                    break;
                }
                case Integer: {
                    // Cast it to an integer
                    types::IntegerType result = metal::strings::stoi(stringSection.c_str(), stringSection.size());
                    row.append(result);
                    break;
                }
                case Integer_opt: {
                    if (stringSection.size() > 0) {
                        types::IntegerType result = metal::strings::stoi(stringSection.c_str(), stringSection.size());
                        row.append(result);
                    }
                    break;
                }
                case Float: {
                    // Cast it to a float.
                    types::FloatType result = metal::strings::stof(stringSection.c_str(), stringSection.size());
                    row.append(result);
                    break;
                }
                case Float_opt: {
                    if (stringSection.size() > 0) {
                        types::FloatType result = metal::strings::stof(stringSection.c_str(), stringSection.size());
                        row.append(result);
                    }
                    break;
                }
                case Unknown:
                    break;
                }
            }

            return row;
        }

    private:
        InstSerializedValuePtr _instructions;

        METAL_CONSTANT static constexpr int MAX_CACHED_COLUMNS = 16;
        mutable METAL_DEVICE char* __cachedColumnStart[MAX_CACHED_COLUMNS];
        mutable RowNumType __lastCachedRow = -1;
        mutable NumColumnsType __lastCachedColumn = -1;
        mutable METAL_DEVICE char* __lastCachedColumnValue = nullptr;

        void AdvanceColumn(METAL_DEVICE char** startOfColumn, RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const {
            // Advances startOfColumn ahead by 1 column.
            if (column == 0) {
                // Fetch it raw.
                const auto rowIndex = rawTable.GetRowIndex(this->skipHeader() ? row+1 : row);
                *startOfColumn = rawTable.data(rowIndex);
            } else {
                *startOfColumn = metal::strings::strchr(*startOfColumn, ',');
                if (*startOfColumn) {
                    (*startOfColumn)++;
                }
            }
        }

        METAL_DEVICE char* GetStartOfColumn(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const {
            METAL_DEVICE char* startOfColumn = nullptr;

            if (row != this->__lastCachedRow) {
                // Null out the cache when we move to a different row
                for (int i = 0; i < MAX_CACHED_COLUMNS; ++i) {
                    this->__cachedColumnStart[i] = nullptr;
                }
                this->__lastCachedColumnValue = nullptr;
                this->__lastCachedColumn = -1;
                this->__lastCachedRow = row;
            }

            // Try sequential
            if (column == this->__lastCachedColumn + 1) {
                // This is a sequential read!
                this->AdvanceColumn(&this->__lastCachedColumnValue, rawTable, row, column);

                // Cache the result!
                // We moved the lastCachedColumnValue, so don't need to update it.
                this->__lastCachedColumn = column;
                if (column < MAX_CACHED_COLUMNS) {
                    this->__cachedColumnStart[column] = this->__lastCachedColumnValue;
                }
                return this->__lastCachedColumnValue;
            }

            if (column > 0 && column < MAX_CACHED_COLUMNS) {
                // Use the cached version
                // Even if it's null, that's ok because we will fetch it again.
                // Don't cache the first column, because we don't need to iterate through the row, so it's very fast.
                startOfColumn = this->__cachedColumnStart[column];
            }

            if (!startOfColumn) {
                // Didn't fetch it from cache, fetch it now by scanning.

                // Find first cached column, within the range of the cache
                NumColumnsType firstCached;
                const auto maxCachedColumn = (column < MAX_CACHED_COLUMNS ? column : (MAX_CACHED_COLUMNS - 1));
                for (firstCached = maxCachedColumn; firstCached > 0; --firstCached) {
                    auto cachedStartOfColumn = this->__cachedColumnStart[firstCached - 1];
                    if (cachedStartOfColumn) {
                        startOfColumn = cachedStartOfColumn;
                        break;
                    }
                }

                // Cache all values from the last cached point.
                NumColumnsType i;
                for (i = firstCached; i <= maxCachedColumn; ++i) {
                    // Fetch it raw.
                    this->AdvanceColumn(&startOfColumn, rawTable, row, i);
                    this->__cachedColumnStart[i] = startOfColumn;
                }

                // Continue past the end of the cached point
                for (; i <= column; ++i) {
                    this->AdvanceColumn(&startOfColumn, rawTable, row, i);
                }

                // Save for serial read
                this->__lastCachedColumn = column;
                this->__lastCachedColumnValue = startOfColumn;
            }
            return startOfColumn;
        }

        StringSection readCSVColumnImpl(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const {
            METAL_DEVICE char* startOfColumn = this->GetStartOfColumn(rawTable, row, column);

            if (!startOfColumn) {
                return StringSection(startOfColumn, 0);
            }

            ColumnSizeType length = 0;
            if (row == rawTable.GetNumRows() - 1 && column == this->numColumns() - 1) {
                length = ((ColumnSizeType) (rawTable.data(rawTable.GetSizeOfData() - 1) - startOfColumn)) + 1;
            } else if (row < rawTable.GetNumRows()) {
                // Only safe to calculate the end of the column if not the last row.
                // Otherwise could read past the end of the buffer.
                METAL_DEVICE char* endOfColumn = metal::strings::strchr(startOfColumn, ',');

                // Unless it's the last row, read until the start of the next row.
                auto startOfNextRowInd = rawTable.GetRowIndex(this->skipHeader() ? row+2 : row+1);
                METAL_DEVICE char* startOfNextRow = rawTable.data(startOfNextRowInd);
                auto lengthOfThisColumn = endOfColumn - startOfColumn;
                auto lengthToNextRow = startOfNextRow - startOfColumn;
                length = lengthOfThisColumn > lengthToNextRow ? lengthToNextRow : lengthOfThisColumn;
            }

            // TODO: Must be matching quotes.
            if ((*startOfColumn == '"' || *startOfColumn == '\'') && (*(startOfColumn+length-1) == '"' || (*(startOfColumn+length-1) == '"'))) {
                // Starts and ends with a quote, strip them.
                return StringSection(startOfColumn + 1, length - 2);
            }

            return StringSection(startOfColumn, length);
        }
    };
}
