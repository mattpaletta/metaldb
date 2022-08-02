#pragma once

#include <string>
#include <vector>
#include <memory>

namespace metaldb::reader {
    class RawTable final {
    public:
        using RowIndexType = uint32_t;

        RawTable(std::vector<char> buffer, std::vector<RowIndexType> rowIndexes, std::vector<std::string> columns) noexcept;
        ~RawTable() noexcept = default;

        /**
         * Creates a placeholder RawTable object that can be filled with data later.
         */
        static std::shared_ptr<RawTable> Placeholder() noexcept;

        /**
         * Returns an empty RawTable object where `IsValid()` returns false.
         */
        static RawTable Invalid() noexcept;

        /**
         * Returns a debug string representation of the RawTable object.
         */
        __attribute__((pure)) std::string DebugStr() const noexcept;

        /**
         * Returns the number of rows in the RawTable object.
         */
        __attribute__((const)) std::uint32_t NumRows() const noexcept;

        /**
         * Returns the number of columns in the RawTable object.
         */
        __attribute__((const)) std::uint32_t NumColumns() const noexcept;

        /**
         * Returns the number of bytes in the RawTable object.
         */
        __attribute__((const)) std::uint32_t NumBytes() const noexcept;

        /**
         * Reads the `i`th row, and returns a string representation.
         * @param row The row to read.  This should be less than `Row::NumRows`
         *
         * If the row is not valid or exceeds `Row::NumRows`, the empty string will be returned.
         */
        __attribute__((pure)) std::string ReadRow(std::size_t row) const noexcept;

        /**
         * Returns true if the RawTable is valid.
         */
        __attribute__((const)) bool IsValid() const noexcept;

        /**
         * A buffer containing the raw data in the RawTable.
         */
        std::vector<char> data;

        /**
         * A list of column names for the table.
         */
        std::vector<std::string> columns;

        /**
         * A list of indexes indicating the start offset in `data` for the ith row.
         */
        std::vector<RowIndexType> rowIndexes;
    private:
        RawTable(bool isValid) noexcept;
        bool _isValid;
    };
}
