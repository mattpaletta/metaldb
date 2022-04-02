//
//  stack.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-23.
//

#pragma once

#include "constants.h"

namespace metaldb {
    template<typename T, size_t N>
    class Stack {
    public:
        Stack() : _data() {}

        bool push(T item) {
            if (this->isFull()) {
                return false;
            }
            this->_data[this->_size++] = item;
            return true;
        }

        T peek() const {
            return this->_data[this->_size - 1];
        }

        T pop() {
            return this->_data[this->_size--];
        }

        bool isEmpty() const {
            return this->_size == 0;
        }

        bool isFull() const {
            return this->_size == N;
        }

    private:
        size_t _size;
        T _data[N];
    };
}
