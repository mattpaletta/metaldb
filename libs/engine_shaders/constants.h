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

#define MAX_VM_STACK_SIZE 100
