#pragma once

#include "Dataframe.hpp"
#include "Instructions.hpp"

#include <metaldb/reader/RawTable.hpp>
#include <iostream>
namespace metaldb::engine {
    class Engine final {
    public:
        Engine() = default;
        ~Engine() = default;

        Dataframe run() {
            return this->runImpl();
        }

    private:
        Dataframe runImpl();
    };
}
