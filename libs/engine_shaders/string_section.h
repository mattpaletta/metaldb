#pragma once

#include "constants.h"

#ifndef __METAL__
#    include <iostream>
#    include <string>
#endif

namespace metaldb {
    template<typename T>
    class GenericStringSection final {
    public:
        using SizeType = uint8_t;

        GenericStringSection(T str, SizeType size) : _size(size), _str(str) {}

        CPP_CONST_FUNC SizeType size() const {
            return this->_size;
        }

        CPP_PURE_FUNC T c_str() const {
            return this->_str;
        }

#ifndef __METAL__
        CPP_PURE_FUNC std::string str() const {
            return std::string(this->c_str(), this->size());
        }

        void print() const {
            for (SizeType i = 0; i < this->size(); ++i) {
                std::cout << this->str()[i];
            }
            std::cout << std::endl;
        }
#endif

    private:
        SizeType _size;
        T _str;
    };

    using StringSection = GenericStringSection<METAL_DEVICE char*>;
    using ConstStringSection = GenericStringSection<const METAL_DEVICE char*>;

    using LocalStringSection = GenericStringSection<METAL_THREAD char*>;
    using ConstLocalStringSection = GenericStringSection<const METAL_THREAD char*>;
}
