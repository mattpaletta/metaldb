#pragma once

#include "constants.h"

namespace metaldb {
    template<types::SizeType N>
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

            for (auto n = 0UL; n < sizeof(T); ++n) {
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

            for (auto n = 0UL; n < sizeof(T); ++n) {
                magicThing.bytes[n] = this->_data[this->_size-- - 1];
            }
            return magicThing.a;
        }

        CPP_CONST_FUNC bool isEmpty() const {
            return this->_size == 0;
        }

        CPP_CONST_FUNC bool isFull() const {
            return this->_size == N;
        }

        CPP_CONST_FUNC types::SizeType size() const {
            return this->_size;
        }

    private:
        types::SizeType _size = 0;
        InstSerializedValue _data[N];
    };
}
