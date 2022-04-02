//
//  column_type.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

namespace metaldb {
    enum ColumnType : instruction_serialized_value_type {
        String,
        Float,
        Integer
    };
}
