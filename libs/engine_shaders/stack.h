//
//  stack.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "constants.h"

namespace metaldb {
    template<size_t N>
    class Stack {
    public:
        Stack() : _data() {}

        template<typename T>
        bool push(T item) {
            if (this->isFull()) {
                return false;
            }

            union {
                T a;
                uint8_t bytes[sizeof(T)];
            } magicThing;

            magicThing.a = item;
            for (auto n = 0UL; n < sizeof(T); ++n) {
                // Insert them in reverse order
                this->_data[this->_size++] = magicThing.bytes[sizeof(T) - n - 1];
            }
            return true;
        }

        template<typename T>
        T peek() const {
            union {
                T a;
                uint8_t bytes[sizeof(T)];
            } magicThing;

            for (auto n = 0; n < sizeof(T); ++n) {
                magicThing.bytes[n] = this->_data[this->_size - 1 - n];
            }
            return magicThing.a;
        }

        template<typename T>
        T pop() {
            union {
                T a;
                uint8_t bytes[sizeof(T)];
            } magicThing;

            for (auto n = 0; n < sizeof(T); ++n) {
                magicThing.bytes[n] = this->_data[this->_size-- - 1];
            }
            return magicThing.a;
        }

        bool isEmpty() const {
            return this->_size == 0;
        }

        bool isFull() const {
            return this->_size == N;
        }

    private:
        size_t _size = 0;
        int8_t _data[N];
    };
}
