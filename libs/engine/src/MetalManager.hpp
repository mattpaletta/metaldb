#pragma once

#include "instruction_type.h"

#import <Metal/Metal.h>
#import <MetalKit/MetalKit.h>

#include <memory>
#include <iostream>
#include <vector>
#include <array>

class MetalManager {
public:
    static std::shared_ptr<MetalManager> Create() {
        MetalManager manager;
        manager.constants = [[MTLFunctionConstantValues alloc] init];

        manager.device = MetalManager::GetDevice();
        if (!manager.device) {
            std::cerr << "Failed to get default device" << std::endl;
            return nullptr;
        }

        std::cout << "Got Device: " << manager.device.description.UTF8String << std::endl;

        std::cout << "Getting Library" << std::endl;
        manager.library = MetalManager::GetLibrary(manager.device);
        if (!manager.library) {
            return nullptr;
        }
        std::cout << "Got Library" << std::endl;

        std::cout << "Compiling Pipeline" << std::endl;
        manager.pipeline = MetalManager::GetComputePipeline(manager.library, manager.constants);
        if (!manager.pipeline) {
            return nullptr;
        }
        std::cout << "Got Pipeline" << std::endl;

        manager.commandQueue = [manager.device newCommandQueue];

        return std::make_shared<MetalManager>(std::move(manager));
    }

    static constexpr auto MAX_OUTPUT_SIZE = 10'000'000;
    using OutputBufferType = std::array<int8_t, MAX_OUTPUT_SIZE>;

    std::size_t MaxNumRows() const {
        return this->pipeline.maxTotalThreadsPerThreadgroup;
    }

    void run(const std::vector<char>& serializedData, const std::vector<metaldb::instruction_serialized_value_type>& instructions, OutputBufferType& outputBuffer, size_t numRows) {
        if (numRows == 0) {
            return;
        }

        // Input buffer
        auto inputBuffer = [this->device newBufferWithLength:serializedData.size() * sizeof(serializedData.at(0))
                                                     options:MTLResourceStorageModeShared];
        auto instructionsBuffer = [this->device newBufferWithLength:(instructions.size() * sizeof(instructions.at(0))) + 1
                                                            options:MTLResourceStorageModeShared];
        auto outputBufferMtl = [this->device newBufferWithLength:outputBuffer.size() options:MTLResourceStorageModeShared];
        std::cout << "Encoding Input Buffer with length: " << inputBuffer.length << std::endl;
        std::cout << "Encoding Instruction Buffer with length: " << instructionsBuffer.length << std::endl;
        std::cout << "Output buffer with length: " << outputBufferMtl.length << std::endl;


        for (std::size_t i = 0; i < serializedData.size(); ++i) {
            ((char*)inputBuffer.contents)[i] = serializedData.at(i);
        }

        for (std::size_t i = 0; i < instructions.size(); ++i) {
            ((int8_t*)instructionsBuffer.contents)[i] = instructions.at(i);
        }

        //            auto captureManager = [MTLCaptureManager sharedCaptureManager];
        //            auto captureDescriptor = [MTLCaptureDescriptor new];
        //            captureDescriptor.captureObject = this->device;
        //
        //            NSError* error = nil;
        //            [captureManager startCaptureWithDescriptor:captureDescriptor error:&error];
        //            if (error) {
        //                std::cerr << "Failed to start capture: " << error.debugDescription.UTF8String << std::endl;
        //            }

        auto commandBuffer = [this->commandQueue commandBuffer];

        MTLSize gridSize = MTLSizeMake(numRows, 1, 1);

        NSUInteger threadGroupSize = this->MaxNumRows();
        if (threadGroupSize > numRows) {
            threadGroupSize = numRows;
        }

        std::cout << "Executing with Grid Size (" << gridSize.width << "," << gridSize.height << "," << gridSize.depth << ")" << std::endl;
        std::cout << "Executing with ThreadGroup Size (" << threadGroupSize << ",1,1)" << std::endl;

        {
            // Send commands to encoder
            id <MTLComputeCommandEncoder> computeEncoder = [commandBuffer computeCommandEncoder];

            [computeEncoder setComputePipelineState:this->pipeline];

            [computeEncoder setBuffer:inputBuffer offset:0 atIndex:0];
            [computeEncoder setBuffer:instructionsBuffer offset:0 atIndex:1];
            [computeEncoder setBuffer:outputBufferMtl offset:0 atIndex:2];

            [computeEncoder dispatchThreads:gridSize threadsPerThreadgroup:MTLSizeMake(threadGroupSize, 1, 1)];
            [computeEncoder endEncoding];
        }

        [commandBuffer commit];
        [commandBuffer waitUntilCompleted];

        auto* contents = (int8_t*) outputBufferMtl.contents;
        for (std::size_t i = 0; i < outputBuffer.size(); ++i) {
            outputBuffer.at(i) = contents[i];
        }
    }

    id<MTLDevice> _Nonnull device;
private:
    MTLFunctionConstantValues* _Nonnull constants;
    id<MTLCommandQueue> _Nonnull commandQueue;
    id<MTLLibrary> _Nonnull library;
    id<MTLComputePipelineState> _Nonnull pipeline;

    static id<MTLDevice> _Nullable GetDevice() {
        auto check = [](id<MTLDevice> device) {
            return device.isLowPower;
        };

        for (id<MTLDevice> device : MTLCopyAllDevices()) {
            if (check(device)) {
                return device;
            }
        }

        return MTLCreateSystemDefaultDevice();
    }

    static id<MTLLibrary> _Nullable GetLibrary(id<MTLDevice> _Nonnull device) {
        NSError* error = nullptr;
        auto library = [device newLibraryWithFile:@"../libs/engine_shaders/MetalDbEngine.metallib" error:&error];

        if (error) {
            std::cerr << "Failed while retrieving library: " << error.debugDescription.UTF8String << std::endl;
            return nullptr;
        }

        return library;
    }

    static id<MTLFunction> _Nullable GetFunction(NSString* _Nonnull funcName, id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) {
        NSError* error = nullptr;
        auto function = [library newFunctionWithName:funcName constantValues:constants error:&error];
        if (error) {
            std::cerr << "Failed while retrieving function: " << error.debugDescription.UTF8String << std::endl;
            return nullptr;
        }

        return function;
    }

    static id<MTLFunction> _Nullable GetEntryFunction(id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) {
        return MetalManager::GetFunction(@"runQueryKernelBackup", library, constants);
    }

    static id<MTLFunction> _Nullable GetInternalBinaryFunction(NSString* _Nonnull funcName, id<MTLLibrary> _Nonnull library) {
        auto functionDescriptor = [MTLFunctionDescriptor new];
        functionDescriptor.name = funcName;
        functionDescriptor.options = MTLFunctionOptionCompileToBinary;
        NSError* error = nullptr;
        auto function = [library newFunctionWithDescriptor:functionDescriptor error:&error];
        if (error) {
            std::cerr << "Failed to compile function: " << funcName.UTF8String << " got error: " << error.debugDescription.UTF8String << std::endl;
            return nullptr;
        }

        return function;
    }

    template<typename ...Args>
    static MTLLinkedFunctions* _Nonnull GetLinkedFunctions(id<MTLLibrary> _Nonnull library, Args... args) {
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

    static id<MTLComputePipelineState> _Nullable GetComputePipeline(id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) {
        auto computeDescriptor = [MTLComputePipelineDescriptor new];
        computeDescriptor.supportAddingBinaryFunctions = true;

        auto linkedFunctions = MetalManager::GetLinkedFunctions(library);

        auto function = MetalManager::GetEntryFunction(library, constants);
        if (!function) {
            std::cerr << "Failed to get entry function: " << std::endl;
            return nullptr;
        }

        computeDescriptor.computeFunction = function;
        computeDescriptor.linkedFunctions = linkedFunctions;

        // A pipeline runs a single function, optionally manipulating the input data before running the function, and the output data afterwards.
        // Creating the pipeline finishes compiling the shader for the GPU.
        NSError* error = nullptr;
        auto pipeline = [library.device newComputePipelineStateWithDescriptor:computeDescriptor
                                                                      options:MTLPipelineOptionNone
                                                                   reflection:nil
                                                                        error:&error];
        if (error) {
            std::cerr << "Failed while creating pipeline: " << error.description.UTF8String << std::endl;
            return nullptr;
        }

        if (linkedFunctions.binaryFunctions.count > 0) {
            auto vft = [MTLVisibleFunctionTableDescriptor new];
            vft.functionCount = linkedFunctions.binaryFunctions.count;
            auto decodeInstructionTable = [pipeline newVisibleFunctionTableWithDescriptor:vft];

            NSUInteger index = 0;
            for (id<MTLFunction> func in linkedFunctions.binaryFunctions) {
                auto functionHandle = [pipeline functionHandleWithFunction:func];
                [decodeInstructionTable setFunction:functionHandle atIndex:index++];
            }
        }
        return pipeline;
    }
};
