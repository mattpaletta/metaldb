#pragma once

#include "constants.h"

#ifdef __METAL__
namespace metal {
    namespace memory {
        template<typename T>
        void memcpyThreadgroupDevice(device T* destination, const device T* source, size_t destIndex, size_t sourceIndex) {
            destination[destIndex] = source[sourceIndex];
            threadgroup_barrier(mem_flags::mem_device);
        }

        template<typename T>
        void memcpyThreadgroupDevice(device T* destination, const threadgroup T* source, size_t index) {
            memcpyThreadgroupDevice(destination, source, index, index);
        }

        template<typename T>
        void memcpyDeviceThreadgroup(threadgroup T* destination, const device T* source, size_t destIndex, size_t sourceIndex) {
            destination[destIndex] = source[sourceIndex];
            threadgroup_barrier(mem_flags::mem_threadgroup);
        }

        template<typename T>
        void memcpyDeviceThreadgroup(threadgroup T* destination, const device T* source, size_t index) {
            memcpyDeviceThreadgroup(destination, source, index, index);
        }

        template<typename T>
        void memcpyDeviceThread(thread T* destination, const device T* source, size_t sourceIndex) {
            *destination = source[sourceIndex];
            simdgroup_barrier(mem_flags::mem_none);
        }

        template<typename T>
        void memcpyThreadDevice(device T* destination, const thread T* source, size_t destIndex) {
            destination[destIndex] = *source;
            simdgroup_barrier(mem_flags::mem_none);
        }
    }
}
#endif
