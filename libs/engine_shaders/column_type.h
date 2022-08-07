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
    
    /**
     * Returns true if a column type is of variable size within a row.
     */
    static bool ColumnVariableSize(ColumnType type) CPP_NOEXCEPT {
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
    
    /**
     * Returns the static column size for columns that are not a variable size.
     * @see ColumnVariableSize
     */
    static uint8_t BaseColumnSize(ColumnType type) CPP_NOEXCEPT {
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
    /**
     * Returns the `ColumnType`, represented as a string.  This is not available in Metal.
     */
    static std::string ColumnTypeToString(ColumnType type) CPP_NOEXCEPT {
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
