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
        
        /**
         * Creates a @b GenericStringSection which stores a starting pointer and a length.
         */
        GenericStringSection(T str, SizeType size) : _size(size), _str(str) {}
        
        /**
         * Returns the length of the string.
         */
        SizeType Size() const {
            return this->_size;
        }
        
        /**
         * Returns the starting pointer as a C string.
         */
        T C_Str() const {
            return this->_str;
        }
        
#ifndef __METAL__
        /**
         * Returns the string as a C++ string.
         */
        std::string Str() const {
            return std::string(this->C_Str(), this->Size());
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
