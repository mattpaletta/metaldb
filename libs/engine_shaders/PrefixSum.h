//
//  PrefixSum.h
//  metaldb
//
//  Created by Matthew Paletta on 2022-04-16.
//

#pragma once

#include "temp_row.h"
#include "constants.h"

#ifdef __METAL__

#include <metal_stdlib>

using value_type = metaldb::TempRow::value_type;

template<ushort LENGTH, typename T>
static inline void ThreadUniformAdd(thread T (&values)[LENGTH], T uni) {
    for (ushort i = 0; i < LENGTH; ++i) {
        values[i] += uni;
    }
}

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
    scratch[local_id] = input + prefix;
}


#endif
