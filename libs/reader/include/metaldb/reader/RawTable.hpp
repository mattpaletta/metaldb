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

        std::vector<char> data;
        std::vector<std::string> columns;
        std::vector<RowIndexType> rowIndexes;

        std::string debugStr() const;
        std::uint32_t numRows() const;
        std::string readRow(std::size_t row) const;

        bool isValid() const;

    private:
        RawTable(bool isValid);
        bool _isValid;
    };
}
