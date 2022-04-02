//
//  string_section.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

namespace metaldb {
    class StringSection final {
    public:
        StringSection(METAL_DEVICE char* str, uint8_t size) : _size(size), _str(str) {}

        uint8_t size() const {
            return this->_size;
        }

        METAL_DEVICE char* str() const {
            return this->_str;
        }

    private:
        uint8_t _size;
        METAL_DEVICE char* _str;
    };
}
