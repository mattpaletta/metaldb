//
//  string_section.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "constants.h"

#ifndef __METAL__
#include <iostream>
#endif

namespace metaldb {
    template<typename T>
    class GenericStringSection final {
    public:
        GenericStringSection(T str, uint8_t size) : _size(size), _str(str) {}

        uint8_t size() const {
            return this->_size;
        }

        METAL_DEVICE char* str() const {
            return this->_str;
        }

#ifndef __METAL__
        void print() const {
            for (int i = 0; i < this->size(); ++i) {
                std::cout << this->str()[i];
            }
            std::cout << std::endl;
        }
#endif

    private:
        uint8_t _size;
        T _str;
    };

    using StringSection = GenericStringSection<METAL_DEVICE char*>;
    using LocalStringSection = GenericStringSection<const METAL_THREAD char*>;
}
