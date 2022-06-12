//
//  column_type.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "instruction_type.h"
#include "constants.h"

namespace metaldb {
    enum ColumnType : InstSerializedValue {
        Unknown,
        String,
        Float,
        Integer
    };

    static bool ColumnVariableSize(ColumnType type) {
        switch (type) {
        case String:
            return true;
        case Float:
        case Integer:
        case Unknown:
            return false;
        }
    }

    static uint8_t BaseColumnSize(ColumnType type) {
        switch (type) {
        case String:
            return 0;
        case Float:
            return sizeof(types::FloatType);
        case Integer:
            return sizeof(types::IntegerType);
        case Unknown:
            return 0;
        }
    }
}
