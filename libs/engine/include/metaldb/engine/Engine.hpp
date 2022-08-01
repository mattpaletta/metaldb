#pragma once

#include "Dataframe.hpp"

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
