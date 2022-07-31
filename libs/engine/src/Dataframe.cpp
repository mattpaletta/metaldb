#include <metaldb/engine/Dataframe.hpp>

auto metaldb::engine::Dataframe::select(const std::vector<Column>& columns, const std::string& table) -> Dataframe& {
    return *this;
}

auto metaldb::engine::Dataframe::filter(const Filter& filter) -> Dataframe& {
    return *this;
}
