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
template<ushort BLOCK_SIZE, typename T>
static T ThreadgroupCooperativePrefixExclusiveSum(T value, threadgroup T* sdata, const ushort lid) {
    // first level of reduction in simdgroup
    T scan = metal::simd_prefix_exclusive_sum(value);

    // store inclusive sums into shared[0...31]
    if ( (lid % 32) == (32 - 1) ) {
        sdata[lid / 32] = scan + value;
    }
    threadgroup_barrier(metal::mem_flags::mem_threadgroup);

    // scan the shared memory
    if (lid < 32) {
        sdata[lid] = metal::simd_prefix_exclusive_sum(sdata[lid]);
    }
    threadgroup_barrier(metal::mem_flags::mem_threadgroup);

    // the scan is the sum of the partial sum prefix scan and the original value
    return scan + sdata[lid / 32];
}

template<ushort BLOCK_SIZE, typename T>
void PrefixScanKernel(threadgroup T* scratch, T input, uint local_id /*[[thread_position_in_threadgroup]]*/) {
    //  scan the aggregates
    T prefix = ThreadgroupCooperativePrefixExclusiveSum<BLOCK_SIZE>(input, scratch, local_id);
    scratch[local_id] = prefix;
}


#endif
