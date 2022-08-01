#pragma once

#include "instruction_type.h"
#include "constants.h"

#ifndef __METAL__
#  include <string>
#endif

namespace metaldb {
    enum ColumnType : InstSerializedValue {
        Unknown,
        String,
        String_opt,
        Float,
        Float_opt,
        Integer,
        Integer_opt,
    };

    CPP_CONST_FUNC static bool ColumnVariableSize(ColumnType type) CPP_NOEXCEPT {
        switch (type) {
        case String:
        case String_opt:
        case Float_opt:
        case Integer_opt:
            return true;
        case Float:
        case Integer:
        case Unknown:
            return false;
        }
    }

    CPP_CONST_FUNC static uint8_t BaseColumnSize(ColumnType type) CPP_NOEXCEPT {
        switch (type) {
        case String:
        case String_opt:
        case Float_opt:
        case Integer_opt:
            return 0;
        case Float:
            return sizeof(types::FloatType);
        case Integer:
            return sizeof(types::IntegerType);
        case Unknown:
            return 0;
        }
    }

#ifndef __METAL__
    CPP_CONST_FUNC static std::string columnTypeToString(ColumnType type) CPP_NOEXCEPT {
        switch (type) {
        case String:
        case String_opt:
            return "String";
        case Float:
        case Float_opt:
            return "Float";
        case Integer:
        case Integer_opt:
            return "Integer";
        case Unknown:
            return "Unknown";
        }
    }
#endif
}
