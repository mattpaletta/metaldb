#pragma once

#include "constants.h"

namespace metaldb {
    template<types::SizeType N>
    class Stack {
    public:
        using SizeType = types::SizeType;
        
        /**
         * Constructs a stack with fixed memory space of N bytes.
         */
        Stack() : _data() {}
        
        /**
         * Pushes an item of type T onto the stack.
         * Returns true if the item was successfully added.
         */
        template<typename T>
        bool Push(T item) {
            if (!this->HasSpace<T>()) {
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
            this->_numItems++;
            return true;
        }
        
        /**
         * Retrieves, but does not remove the top item of type T from the stack.
         * The caller is responsible for passing in the right template type when fetching items from the stack.
         * Otherwise, this is considered undefined behaviour.
         */
        template<typename T>
        T Peek() const {
            union {
                T a;
                uint8_t bytes[sizeof(T)];
            } magicThing;
            
            for (auto n = 0UL; n < sizeof(T); ++n) {
                magicThing.bytes[n] = this->_data[this->_size - 1 - n];
            }
            return magicThing.a;
        }
        
        /**
         * Retrieves, and removes the top item of type T from the stack.
         * The caller is responsible for passing in the right template type when fetching items from the stack.
         * Otherwise, this is considered undefined behaviour.
         */
        template<typename T>
        T Pop() {
            union {
                T a;
                uint8_t bytes[sizeof(T)];
            } magicThing;
            
            for (auto n = 0UL; n < sizeof(T); ++n) {
                magicThing.bytes[n] = this->_data[this->_size-- - 1];
            }
            this->_numItems--;
            return magicThing.a;
        }
        
        /**
         * Returns true if the stack is empty.
         */
        bool IsEmpty() const {
            return this->_size == 0;
        }
        
        /**
         * Returns true if the stack is full.
         */
        bool IsFull() const {
            return this->_size == N;
        }
        
        /**
         * Returns true if there is space for item of type T on the stack.
         */
        template<typename T>
        bool HasSpace() const {
            return this->_size + sizeof(T) < N;
        }
        
        /**
         * Returns the current size of the stack in bytes.
         */
        SizeType Size() const {
            return this->_size;
        }
        
        /**
         * Returns the maximum capacity of the stack in bytes.
         */
        SizeType Capacity() const {
            return N;
        }
        
        /**
         * Returns the number of items on the stack.
         */
        SizeType NumItems() const {
            return this->_numItems;
        }
        
    private:
        SizeType _size = 0;
        SizeType _numItems = 0;
        InstSerializedValue _data[N];
    };
}
