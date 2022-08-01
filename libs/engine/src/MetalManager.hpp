#pragma once

#include "instruction_type.h"
#include "engine.h"

#import <Metal/Metal.h>
#import <MetalKit/MetalKit.h>

#include <memory>
#include <iostream>
#include <vector>
#include <array>

namespace metaldb {
    class MetalManager {
    public:
        static std::shared_ptr<MetalManager> Create() noexcept;

        static constexpr auto MAX_OUTPUT_SIZE = 1'000'000;
        using OutputBufferType = std::array<int8_t, MAX_OUTPUT_SIZE>;

        CPP_CONST_FUNC std::size_t MaxNumRows() const noexcept;

        CPP_CONST_FUNC std::size_t MaxMemory() const noexcept;

        void runCPU(const std::vector<char>& serializedData, const std::vector<metaldb::InstSerializedValue>& instructions, OutputBufferType& outputBuffer, size_t numRows) noexcept;

        void run(const std::vector<char>& serializedData, const std::vector<metaldb::InstSerializedValue>& instructions, OutputBufferType& outputBuffer, size_t numRows) noexcept;

        id<MTLDevice> _Nonnull device;
    private:
        MTLFunctionConstantValues* _Nonnull constants;
        id<MTLCommandQueue> _Nonnull commandQueue;
        id<MTLLibrary> _Nonnull library;
        id<MTLComputePipelineState> _Nonnull pipeline;

        static id<MTLDevice> _Nullable GetDevice() noexcept;

        static id<MTLLibrary> _Nullable GetLibrary(id<MTLDevice> _Nonnull device) noexcept;

        static id<MTLFunction> _Nullable GetFunction(NSString* _Nonnull funcName, id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) noexcept;

        static id<MTLFunction> _Nullable GetEntryFunction(id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) noexcept;

        static id<MTLFunction> _Nullable GetInternalBinaryFunction(NSString* _Nonnull funcName, id<MTLLibrary> _Nonnull library) noexcept;

        template<typename ...Args>
        static MTLLinkedFunctions* _Nonnull GetLinkedFunctions(id<MTLLibrary> _Nonnull library, Args... args) noexcept {
            NSMutableArray<id<MTLFunction>>* binaryFunctionsArr = [NSMutableArray new];

            if constexpr(sizeof...(args) > 0) {
                // Only run the loop if at least 1 arg.
                for (NSString* arg : {args...}) {
                    auto function = MetalManager::GetInternalBinaryFunction(arg, library);
                    if (function) {
                        [binaryFunctionsArr addObject:function];
                    } else {
                        std::cerr << "Failed to add function: " << arg << " to list of compiled functions." << std::endl;
                    }
                }
            }

            auto linkedFunctions = [MTLLinkedFunctions new];
            linkedFunctions.binaryFunctions = binaryFunctionsArr;
            return linkedFunctions;
        }

        static id<MTLComputePipelineState> _Nullable GetComputePipeline(id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) noexcept;
    };
}
