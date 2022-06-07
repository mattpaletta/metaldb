#pragma once

#include "Dataframe.hpp"
#include "Instructions.hpp"

#include <metaldb/reader/RawTable.hpp>
#include <iostream>
namespace metaldb::engine {
    class Engine final {
    public:
        Engine() = default;
        ~Engine() {
            std::cout << "Destroying Engine" << std::endl;
        };


        template<typename... Args>
        Dataframe run(const reader::RawTable& rawTable, const Args& ...args) {
            Encoder encoder;
            this->encodeAll(encoder, args...);
            auto encoded = encoder.data();
            return this->runImpl(rawTable, std::move(encoded));
        }

    private:
        Dataframe runImpl(const reader::RawTable& rawTable, instruction_serialized_type&& instructions);

        template <class T, class... Ts>
        void encodeAll(Encoder& encoder, const T& first, const Ts&... rest) {
            encoder.encode(first);

            if constexpr (sizeof...(rest) > 0) {
                encodeAll(encoder, rest...);
            }
        }
    };
}
