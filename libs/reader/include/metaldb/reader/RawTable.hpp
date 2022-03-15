//
//  RawTable.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-05.
//

#pragma once

#include <string>
#include <vector>

namespace metaldb::reader {
    class RawTable {
    public:
        RawTable(std::vector<char>&& buffer, const std::vector<std::size_t>& rowIndexes, const std::vector<std::string>& columns);

        static RawTable invalid();

        std::vector<char> data;
        std::vector<std::string> columns;
        std::vector<std::size_t> rowIndexes;

        std::string debugStr() const;
        std::size_t numRows() const;
        std::string readRow(std::size_t row) const;

        bool isValid() const;

    private:
        RawTable(bool isValid);
        bool _isValid;

    };
}
