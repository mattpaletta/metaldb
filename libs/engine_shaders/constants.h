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
#endif


// TODO: Make this a function constant in Metal so we can change it.
#define MAX_VM_STACK_SIZE 32

// 1 kilobyte
#define MAX_OUTPUT_ROW_LENGTH 1024

using InstructionPtr = uint64_t;

namespace metaldb {
    namespace types {
        using IntegerType = int64_t;
        using FloatType = float;
        using StringType = char;

        using SizeType = uint64_t;
    }
}
