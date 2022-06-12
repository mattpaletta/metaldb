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

#define CPP_RESTRICT
#else
#define METAL_CONSTANT
#define METAL_DEVICE
#define METAL_THREAD
#define METAL_THREADGROUP
#define METAL_KERNEL
#define METAL_VISIBLE
#define CPP_RESTRICT restrict
#endif


#ifndef __METAL__
#include <cstdint>
#include <vector>
#endif


// TODO: Make this a function constant in Metal so we can change it.
#define MAX_VM_STACK_SIZE 32

// 1 kilobyte
#define MAX_OUTPUT_ROW_LENGTH 1024

using InstructionPtr = uint64_t;

namespace metaldb {

    using InstSerializedValue = int8_t;
    using InstSerializedValuePtr = METAL_DEVICE InstSerializedValue*;
    using OutputSerializedValue = int8_t;

    namespace types {
        using IntegerType = int64_t;
        using FloatType = float;
        using StringType = char;

        using SizeType = uint64_t;
    }

    template<typename Val, typename T>
    static Val ReadBytesStartingAt(T METAL_DEVICE * ptr) {
        if constexpr(sizeof(Val) == sizeof(T)) {
            return (Val) *ptr;
        } else {
            return *((Val METAL_DEVICE *) ptr);
        }
    }

    template<typename Val, typename T>
    static void WriteBytesStartingAt(T METAL_DEVICE * ptr, const Val METAL_THREAD & val) {
        if constexpr(sizeof(T) == sizeof(Val)) {
            *ptr = val;
        } else {
            *((Val METAL_DEVICE *) ptr) = val;
        }
    }

#ifndef __METAL__
    template<typename T, typename Val>
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
