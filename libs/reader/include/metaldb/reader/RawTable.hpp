//
//  RawTable.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-05.
//

#pragma once

#include <string>
#include <vector>
#include <memory>

namespace metaldb::reader {
    class RawTable {
    public:
        using RowIndexType = uint32_t;

        RawTable(std::vector<char>&& buffer, const std::vector<RowIndexType>& rowIndexes, const std::vector<std::string>& columns);

        static std::shared_ptr<RawTable> placeholder();
        static RawTable invalid();

        __attribute__((pure)) std::string debugStr() const;
        __attribute__((const)) std::uint32_t numRows() const;
        __attribute__((pure)) std::string readRow(std::size_t row) const;

        __attribute__((const)) bool isValid() const;

        std::vector<char> data;
        std::vector<std::string> columns;
        std::vector<RowIndexType> rowIndexes;
    private:
        RawTable(bool isValid);
        bool _isValid;
    };
}
