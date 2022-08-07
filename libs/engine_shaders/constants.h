#pragma once

#ifdef __METAL_VERSION__
#include <metal_stdlib>

#define __METAL__

#define METAL_CONSTANT constant
#define METAL_DEVICE device
#define METAL_THREAD thread
#define METAL_THREADGROUP threadgroup
#define METAL_KERNEL kernel
#define METAL_VISIBLE [[visible]]

#define CPP_NOEXCEPT
#define CPP_CONSTEXPR

#else
#define METAL_CONSTANT
#define METAL_DEVICE
#define METAL_THREAD
#define METAL_THREADGROUP
#define METAL_KERNEL
#define METAL_VISIBLE
#define CPP_NOEXCEPT noexcept
#define CPP_CONSTEXPR constexpr
#endif

#ifndef __METAL__
#include <cstdint>
#include <vector>
#endif

using InstructionPtr = uint64_t;

namespace metaldb {
    
    using InstSerializedValue = int8_t;
    using InstSerializedValuePtr = METAL_DEVICE InstSerializedValue*;
    using OutputSerializedValue = int8_t;
    
    METAL_CONSTANT static constexpr auto MAX_OUTPUT_ROW_LENGTH = 1024;
    
    namespace types {
        using IntegerType = int64_t;
        using FloatType = float;
        using StringType = char;
        
        using SizeType = uint64_t;
    }
    
    /**
     * Reads `sizeof(Val)` bytes starting from @b ptr , and casts the return value as @b Val.
     * @param ptr The starting pointer to read from.
     *
     * The caller should ensure that the bytes to read are in fact of type `Val` and the pointer is not null.
     */
    template<typename Val, typename T>
    static Val ReadBytesStartingAt(T METAL_DEVICE * ptr) CPP_NOEXCEPT {
        if CPP_CONSTEXPR(sizeof(Val) == sizeof(T)) {
            return (Val) *ptr;
        } else {
            return *((Val METAL_DEVICE *) ptr);
        }
    }
    
#ifdef __METAL__
    /**
     * Reads `sizeof(Val)` bytes starting from @b ptr , and casts the return value as @b Val.
     * @param ptr The starting pointer to read from.
     *
     * The caller should ensure that the bytes to read are in fact of type `Val` and the pointer is not null.
     */
    template<typename Val, typename T>
    static Val ReadBytesStartingAt(T METAL_THREAD * ptr) CPP_NOEXCEPT {
        if CPP_CONSTEXPR(sizeof(Val) == sizeof(T)) {
            return (Val) *ptr;
        } else {
            return *((Val METAL_THREAD *) ptr);
        }
    }
#endif
    
    /**
     * Writes a value into a pointer which may be of different size byte by byte.
     * @param ptr The pointer to start writing bytes
     * @param val The value to write to the pointer
     *
     * The caller should ensure the pointer is not null and the next N bytes are also available to write to.
     * Where N is the `sizeof(Val)`
     */
    template<typename Val, typename T>
    static void WriteBytesStartingAt(T METAL_DEVICE * ptr, const Val METAL_THREAD & val) CPP_NOEXCEPT {
        if CPP_CONSTEXPR(sizeof(T) == sizeof(Val)) {
            *ptr = val;
        } else {
            for (size_t n = 0; n < (sizeof(Val) / sizeof(T)); ++n) {
                *(ptr++) = (T)(val >> (8 * n)) & 0xff;
            }
        }
    }
    
#ifdef __METAL__
    /**
     * Writes a value into a pointer which may be of different size byte by byte.
     * @param ptr The pointer to start writing bytes
     * @param val The value to write to the pointer
     *
     * The caller should ensure the pointer is not null and the next N bytes are also available to write to.
     * Where N is the `sizeof(Val)`
     */
    template<typename Val, typename T>
    static void WriteBytesStartingAt(T METAL_THREAD * ptr, const Val METAL_THREAD & val) CPP_NOEXCEPT {
        if CPP_CONSTEXPR(sizeof(T) == sizeof(Val)) {
            *ptr = val;
        } else {
            for (size_t n = 0; n < (sizeof(Val) / sizeof(T)); ++n) {
                *(ptr++) = (T)(val >> (8 * n)) & 0xff;
            }
        }
    }
#endif
    
#ifndef __METAL__
    /**
     * Writes a value into a vector which may be of different size byte by byte.
     * @param ptr The vector to start writing bytes
     * @param val The value to write to the vector
     */
    template<typename Val, typename T>
    static void WriteBytesStartingAt(std::vector<T>& ptr, const Val& val) CPP_NOEXCEPT {
        union {
            Val a;
            T bytes[sizeof(Val)];
        } thing;
        
        thing.a = val;
        
        for (auto n = 0UL; n < sizeof(Val); ++n) {
            ptr.emplace_back(thing.bytes[n]);
        }
    }
#endif
}
