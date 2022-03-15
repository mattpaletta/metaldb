//
//  Dataframe.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-06.
//

#pragma once

#include "Column.hpp"
#include "Filter.hpp"

#include <vector>
#include <string>

namespace metaldb::engine {
    class Dataframe {
    public:
        Dataframe() = default;

        template<typename... Args>
        Dataframe& select(Args... columns, const std::string& table) {
            return this->select({{columns...}}, table);
        }

        Dataframe& select(const std::vector<Column>& columns, const std::string& table);
        Dataframe& filter(const Filter& filter);

    private:

    };
}
