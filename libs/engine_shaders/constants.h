#pragma once

#ifdef __METAL_VERSION__
#include <metal_stdlib>

#define __METAL__

#define METAL_CONSTANT constant
#define METAL_DEVICE device
#define METAL_THREAD thread
#define METAL_THREADGROUP threadgroup
#define CPP_RESTRICT
#else
#define METAL_CONSTANT
#define METAL_DEVICE
#define METAL_THREAD
#define METAL_THREADGROUP
#define CPP_RESTRICT restrict
#endif


#ifndef __METAL__
#include <cstdint>
#endif

#define MAX_VM_STACK_SIZE 100

// TODO: Make this a function constant in Metal so we can change it.
// 1 kilobyte
#define MAX_OUTPUT_ROW_LENGTH 1024
