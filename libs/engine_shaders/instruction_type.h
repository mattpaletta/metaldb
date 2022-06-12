#pragma once

#include "constants.h"

namespace metaldb {
    enum InstructionType : InstSerializedValue {
        PARSEROW,
        PROJECTION,
        FILTER,
        OUTPUT,
    };
}
