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

#define CPP_CONSTEXPR
#define CPP_RESTRICT
#define CPP_PURE_FUNC
#define CPP_CONST_FUNC

#else
#define METAL_CONSTANT
#define METAL_DEVICE
#define METAL_THREAD
#define METAL_THREADGROUP
#define METAL_KERNEL
#define METAL_VISIBLE
#define CPP_CONSTEXPR constexpr
#define CPP_RESTRICT restrict
#define CPP_PURE_FUNC __attribute__((pure))
#define CPP_CONST_FUNC __attribute__((const))
#endif

#ifndef __METAL__
#include <cstdint>
#include <vector>
#include <cassert>
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

    template<typename Val, typename T>
    static Val ReadBytesStartingAt(T METAL_DEVICE * ptr) {
        if CPP_CONSTEXPR(sizeof(Val) == sizeof(T)) {
            return (Val) *ptr;
        } else {
            return *((Val METAL_DEVICE *) ptr);
        }
    }

#ifdef __METAL__
    template<typename Val, typename T>
    static Val ReadBytesStartingAt(T METAL_THREAD * ptr) {
        if CPP_CONSTEXPR(sizeof(Val) == sizeof(T)) {
            return (Val) *ptr;
        } else {
            return *((Val METAL_THREAD *) ptr);
        }
    }
#endif

    template<typename Val, typename T>
    static void WriteBytesStartingAt(T METAL_DEVICE * ptr, const Val METAL_THREAD & val) {
        if CPP_CONSTEXPR(sizeof(T) == sizeof(Val)) {
            *ptr = val;
        } else {
            for (size_t n = 0; n < (sizeof(Val) / sizeof(T)); ++n) {
                *(ptr++) = (T)(val >> (8 * n)) & 0xff;
            }
        }
    }

#ifdef __METAL__
    template<typename Val, typename T>
    static void WriteBytesStartingAt(T METAL_THREAD * ptr, const Val METAL_THREAD & val) {
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
    template<typename Val, typename T>
    static void WriteBytesStartingAt(std::vector<T>& ptr, const Val& val) {
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
