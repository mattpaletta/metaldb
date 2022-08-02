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
    /**
     * Parses an input buffer using a `Method` and then the Nth thread writes the Nth row as a @b TempRow .
     */
    class ParseRowInstruction {
    public:
        /**
         * The type to store the method for parsing.
         */
        using MethodType = Method;

        /**
         * The starting position of the method.
         */
        METAL_CONSTANT static constexpr auto MethodOffset = 0;

        /**
         * The type to store if we should skip the header when parsing.
         */
        using SkipHeaderType = bool;

        /**
         * The starting position of the skip header flag.
         */
        METAL_CONSTANT static constexpr auto SkipHeaderOffset = sizeof(MethodType) + MethodOffset;

        /**
         * The type to store the number of columns to parse.
         */
        using NumColumnsType = OutputRow::NumColumnsType;

        /**
         * The starting position for the number of columns to parse.
         */
        METAL_CONSTANT static constexpr auto NumColumnsOffset = sizeof(SkipHeaderType) + SkipHeaderOffset;

        /**
         * The starting position for the column type of each column when parsing.
         */
        METAL_CONSTANT static constexpr auto ColumnTypeOffset = sizeof(NumColumnsType) + NumColumnsOffset;

        /**
         * The type to store the size of each column.
         */
        using ColumnSizeType = TempRow::ColumnSizeType;

        /**
         * The type to keep track of the current row number.
         */
        using RowNumType = uint16_t;

#ifndef __METAL__
        ParseRowInstruction() CPP_NOEXCEPT : _instructions(nullptr) {}
#endif
        // Pointer points to beginning of ParseRow instruction.
        ParseRowInstruction(InstSerializedValuePtr instructions) CPP_NOEXCEPT : _instructions(instructions) {}

        CPP_PURE_FUNC MethodType GetMethod() const CPP_NOEXCEPT {
            return ReadBytesStartingAt<MethodType>(&this->_instructions[MethodOffset]);
        }

        CPP_PURE_FUNC SkipHeaderType SkipHeader() const CPP_NOEXCEPT {
            return ReadBytesStartingAt<SkipHeaderType>(&this->_instructions[SkipHeaderOffset]);
        }

        CPP_PURE_FUNC NumColumnsType NumColumns() const CPP_NOEXCEPT {
            return ReadBytesStartingAt<NumColumnsType>(&this->_instructions[NumColumnsOffset]);
        }

        CPP_PURE_FUNC ColumnType GetColumnType(NumColumnsType index) const CPP_NOEXCEPT {
            return ReadBytesStartingAt<ColumnType>(&this->_instructions[(index * sizeof(ColumnType)) + ColumnTypeOffset]);
        }

        /**
         * Returns a pointer 1 past the end of the parse instruction.  This will either be an unknown if we exceed the end of the array or
         * an encoded @b InstructionType .
         */
        CPP_PURE_FUNC InstSerializedValuePtr End() const CPP_NOEXCEPT {
            // Returns 1 past the end of the instruction
            const auto numColumns = this->NumColumns();
            const auto offset = ColumnTypeOffset + (numColumns * sizeof(ColumnType));
            return &this->_instructions[offset];
        }

        /**
         * Parses the jth column from the nth row.
         * If the column could not be read, an empty string is returned.
         *
         * @param rawTable The @b RawTable to read from.  This @b RawTable must be in CSV format.
         * @param row The row to read.  This generally corresponds to the thread number, but does not have to.  It is up to the caller
         *           to ensure that the row is a valid row in the @b rawTable .
         * @param column The column to read from the row.  It is up to the caller to ensure that the column requested is less
         *               than @b NumColumns .
         *
         * @note This class utilizes internal caching for significantly better performance on incrementing column numbers.
         */
        CPP_PURE_FUNC StringSection ReadCSVColumn(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const CPP_NOEXCEPT {
            return this->ReadCSVColumnImpl(rawTable, row, column);
        }

        /**
         * Returns the length of the CSV column.
         * If the column could not be read, 0 is returned.
         *
         * @see ParseRowInstruction::ReadCSVColumn
         *
         * @note This class utilizes internal caching for significantly better performance on incrementing column numbers.
         */
        CPP_PURE_FUNC ColumnSizeType ReadCSVColumnLength(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const CPP_NOEXCEPT {
            // Do it this way for now to keep the code the same.
            return this->ReadCSVColumn(rawTable, row, column).Size();
        }

        /**
         * Returns the ith row, based on the the thread number in @b constants as a @b TempRow .
         *
         * @param constants The struct of constants to use as temporary storage.
         */
        CPP_PURE_FUNC TempRow GetRow(DbConstants METAL_THREAD & constants) CPP_NOEXCEPT {
            TempRow::TempRowBuilder builder;
            auto numCols = this->NumColumns();
            {
                builder.numColumns = numCols;

                // Read column types
                for (auto i = 0; i < numCols; ++i) {
                    // Set all column types
                    auto columnType = this->GetColumnType(i);
                    builder.columnTypes[i] = columnType;

                    // Set all column sizes, and they might get pruned
                    if (ColumnVariableSize(columnType)) {
                        builder.columnSizes[i] = this->ReadCSVColumnLength(constants.rawTable, constants.thread_position_in_threadgroup, i);
                    } else {
                        builder.columnSizes[i] = 0;
                    }
                }
            }

            // Populate the row.
            TempRow row = builder;
            for (auto i = 0; i < numCols; ++i) {
                // Write the columns into the buffer
                auto stringSection = this->ReadCSVColumn(constants.rawTable, constants.thread_position_in_threadgroup, i);

                switch (this->GetColumnType(i)) {
                case String:
                case String_opt: {
                    // Copy the string in directly.
                    row.Append(stringSection.C_Str(), stringSection.Size());
                    break;
                }
                case Integer: {
                    // Cast it to an integer
                    types::IntegerType result = metal::strings::stoi(stringSection.C_Str(), stringSection.Size());
                    row.Append(result);
                    break;
                }
                case Integer_opt: {
                    if (stringSection.Size() > 0) {
                        types::IntegerType result = metal::strings::stoi(stringSection.C_Str(), stringSection.Size());
                        row.Append(result);
                    }
                    break;
                }
                case Float: {
                    // Cast it to a float.
                    types::FloatType result = metal::strings::stof(stringSection.C_Str(), stringSection.Size());
                    row.Append(result);
                    break;
                }
                case Float_opt: {
                    if (stringSection.Size() > 0) {
                        types::FloatType result = metal::strings::stof(stringSection.C_Str(), stringSection.Size());
                        row.Append(result);
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

        /**
         * Sets the `startOfColumn` pointer to the beginning of the @b column for the @b row .
         *
         * @note This method does not utilize any caching and should not be called directly.
         */
        CPP_PURE_FUNC void AdvanceColumn(METAL_DEVICE char** startOfColumn, RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const CPP_NOEXCEPT {
            // Advances startOfColumn ahead by 1 column.
            if (column == 0) {
                // Fetch it raw.
                const auto rowIndex = rawTable.GetRowIndex(this->SkipHeader() ? row+1 : row);
                *startOfColumn = rawTable.Data(rowIndex);
            } else {
                *startOfColumn = metal::strings::strchr(*startOfColumn, ',');
                if (*startOfColumn) {
                    (*startOfColumn)++;
                }
            }
        }

        /**
         * Returns a pointer to the beginning of the @b column for the @b row .
         * This method uses 2 levels of caching.  The first is block-based caching.
         * The first @b MAX_CACHED_COLUMNS are stored in an array within the class as they are computed, so there is no
         * wasted work.
         * If any of those columns are re-requested, we fetch the value from the cache and return it.  If the value is not yet cached,
         * we will jump to the last column we do have cached, and scan sequentially from there caching each value from there.
         *
         * The other type of caching is sequential caching.  In case the column is one more than the last column computed, we store the
         * last column returned as a pointer.  This means we only have to read the one column to return a value.  If the colum is within the
         * @b MAX_CACHED_COLUMNS , we also store the pointer in the block-based cache.  If the column requested is not one more
         * than the last returned column, and the column is outside of the @b MAX_CACHED_COLUMNS , we start with the highest
         * block-cached value, and scan sequentially for the value, storing the pointer to this last column before returning it.
         *
         * The combination of caches should ensure good performance in the most common use case, which is sequential reads,
         * or random access where rows have less than @b MAX_CACHED_COLUMNS without taking too much space or time.
         *
         * When we move to a new row, we reset both cache.
         */
        CPP_PURE_FUNC METAL_DEVICE char* GetStartOfColumn(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const CPP_NOEXCEPT {
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

        /**
         * Implements reading a row and column from a @b RawTable and returning the column in a @b StringSection .
         *
         * If the column could not be read, an empty string is returned.  If the column contains quotes, they will be stripped off.
         */
        CPP_PURE_FUNC StringSection ReadCSVColumnImpl(RawTable METAL_THREAD & rawTable, RowNumType row, NumColumnsType column) const CPP_NOEXCEPT {
            METAL_DEVICE char* startOfColumn = this->GetStartOfColumn(rawTable, row, column);

            if (!startOfColumn) {
                return StringSection(startOfColumn, 0);
            }

            ColumnSizeType length = 0;
            if (row == rawTable.GetNumRows() - 1 && column == this->NumColumns() - 1) {
                // The size of the last row/column is the start of the column
                // until the end of the buffer, because there is no next row to fetch.
                length = ((ColumnSizeType) (rawTable.Data(rawTable.GetSizeOfData() - 1) - startOfColumn)) + 1;

            } else if (row < rawTable.GetNumRows()) {
                // Only safe to calculate the end of the column if not the last row.
                // Otherwise could read past the end of the buffer.
                METAL_DEVICE char* endOfColumn = metal::strings::strchr(startOfColumn, ',');

                // Unless it's the last row, read until the start of the next row.
                auto startOfNextRowInd = rawTable.GetRowIndex(this->SkipHeader() ? row+2 : row+1);
                METAL_DEVICE char* startOfNextRow = rawTable.Data(startOfNextRowInd);
                auto lengthOfThisColumn = endOfColumn - startOfColumn;
                auto lengthToNextRow = startOfNextRow - startOfColumn;
                length = lengthOfThisColumn > lengthToNextRow ? lengthToNextRow : lengthOfThisColumn;
            }

            const auto endOfColumn = startOfColumn + length - 1;
            if ((*startOfColumn == '"' && *endOfColumn == '"') || (*startOfColumn == '\'' && *endOfColumn == '\'')) {
                // Starts and ends with a quote, strip them.
                return StringSection(startOfColumn + 1, length - 2);
            }

            return StringSection(startOfColumn, length);
        }
    };
}
