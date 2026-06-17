#pragma once

#include <array>

namespace metaldb::Constants {
    static constexpr auto MAX_OUTPUT_SIZE = 1'000'000;
    using OutputBufferType = std::array<int8_t, MAX_OUTPUT_SIZE>;
}
