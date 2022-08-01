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

        static std::shared_ptr<RawTable> Placeholder() noexcept;
        static RawTable Invalid() noexcept;

        __attribute__((pure)) std::string DebugStr() const noexcept;
        __attribute__((const)) std::uint32_t NumRows() const noexcept;
        __attribute__((const)) std::uint32_t NumColumns() const noexcept;
        __attribute__((const)) std::uint32_t NumBytes() const noexcept;

        __attribute__((pure)) std::string ReadRow(std::size_t row) const noexcept;

        __attribute__((const)) bool IsValid() const noexcept;

        std::vector<char> data;
        std::vector<std::string> columns;
        std::vector<RowIndexType> rowIndexes;
    private:
        RawTable(bool isValid) noexcept;
        bool _isValid;
    };
}
