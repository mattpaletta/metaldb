//
//  PrefixSum.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-04-16.
//

#pragma once

#include "constants.h"

#ifdef __METAL__

#include <metal_stdlib>

// Based on: https://kieber-emmons.medium.com/efficient-parallel-prefix-sum-in-metal-for-apple-m1-9e60b974d62

//------------------------------------------------------------------------------------------------//
//  Cooperative threadgroup scan
template<uint32_t BLOCK_SIZE, typename T>
static T ThreadgroupCooperativePrefixExclusiveSum(T value, threadgroup T* sdata, const uint32_t lid, ushort simdWidth) {
    // first level of reduction in simdgroup
    T scan = metal::simd_prefix_exclusive_sum(value);

    if (lid < BLOCK_SIZE) {
        sdata[lid] = 0;
    }

    // store inclusive sums into shared[0...31]
    const auto threadMod = (lid % simdWidth);
    const auto sharedDataIndex = lid / simdWidth;
    const auto sharedDataValue = scan + value;

    if (threadMod == (simdWidth - 1)) {
        sdata[sharedDataIndex] = sharedDataValue;
    }
    threadgroup_barrier(metal::mem_flags::mem_threadgroup);

    // scan the shared memory
    if (lid < simdWidth) {
        const T value = sdata[lid];
        const T result = metal::simd_prefix_exclusive_sum(value);
        sdata[lid] = result;
    }
    threadgroup_barrier(metal::mem_flags::mem_threadgroup);

    // the scan is the sum of the partial sum prefix scan and the original value
    const auto prefix = sdata[lid / simdWidth];
    return scan + prefix;
}

template<uint32_t BLOCK_SIZE, typename T>
void PrefixScanKernel(threadgroup T* scratch, T input, uint32_t local_id /*[[thread_position_in_threadgroup]]*/, ushort simdWidth /* [[ thread_execution_width ]]*/) {
    //  scan the aggregates
    T prefix = ThreadgroupCooperativePrefixExclusiveSum<BLOCK_SIZE>(input, scratch, local_id, simdWidth);
    scratch[local_id] = prefix;
}

template<uint32_t BLOCK_SIZE, typename T>
T ThreadGroupReduceCooperativeAlgorithm(threadgroup T* scratch, T value, uint32_t local_id /*[[thread_position_in_threadgroup]]*/, ushort simdWidth /* [[ thread_execution_width ]]*/) {
    // First level of reduction in simdgroup
    T simdAdd = metal::simd_sum(value);

    if (local_id < BLOCK_SIZE) {
        scratch[local_id] = 0;
    }

    if (local_id % simdWidth == 0) {
        // First thread writes to local memory
        scratch[local_id / simdWidth] = simdAdd;
    }
    threadgroup_barrier(metal::mem_flags::mem_threadgroup);

    if (local_id < simdWidth) {
        // Mask values
        simdAdd = (local_id < BLOCK_SIZE / simdWidth) ? scratch[local_id] : 0;

        simdAdd = metal::simd_sum(simdAdd);
    }

    return simdAdd;
}


#endif
