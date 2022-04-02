//
//  vm.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "stack.h"

namespace metaldb {
    class VM {
    public:
        VM() = default;
        ~VM() = default;

        void run() {}
    private:
        Stack<int8_t, MAX_VM_STACK_SIZE> _stack;
    };
}
