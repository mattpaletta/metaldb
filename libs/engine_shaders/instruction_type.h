//
//  instruction_type.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#ifndef __METAL__
#include <cstdint>
#endif

namespace metaldb {
    using instruction_serialized_value_type = uint8_t;

    enum InstructionType : instruction_serialized_value_type {
        PARSEROW,
        PROJECTION,
        FILTER,
        OUTPUT,
    };
}
