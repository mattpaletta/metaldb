#include <metaldb/engine/Engine.hpp>

#include "engine.h"

#import <Metal/Metal.h>
#import <MetalKit/MetalKit.h>

#include <memory>
#include <iostream>
#include <array>

namespace {
    class MetalManager {
    public:
        static std::unique_ptr<MetalManager> Create() {
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

            return std::make_unique<MetalManager>(std::move(manager));
        }

        template<size_t MAX_OUTPUT_SIZE = 1'000'000>
        void run(const std::vector<char>& serializedData, const std::vector<int8_t>& instructions, std::array<int8_t, MAX_OUTPUT_SIZE>& outputBuffer, size_t numRows) {
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
//
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

            NSUInteger threadGroupSize = this->pipeline.maxTotalThreadsPerThreadgroup;
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

        id<MTLDevice> device;
    private:
        MTLFunctionConstantValues* constants;
        id<MTLCommandQueue> commandQueue;
        id<MTLLibrary> library;
        id<MTLComputePipelineState> pipeline;

        static id<MTLDevice> GetDevice() {
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

        static id<MTLLibrary> GetLibrary(id<MTLDevice> device) {
            NSError* error = nil;
            auto library = [device newLibraryWithFile:@"../libs/engine_shaders/MetalDbEngine.metallib" error:&error];

            if (error) {
                std::cerr << "Failed while retrieving library: " << error.debugDescription.UTF8String << std::endl;
                return nil;
            }

            return library;
        }

        static id<MTLFunction> GetFunction(NSString* _Nonnull funcName, id<MTLLibrary> library, MTLFunctionConstantValues* constants) {
            NSError* error = nil;
            auto function = [library newFunctionWithName:funcName constantValues:constants error:&error];
            if (error) {
                std::cerr << "Failed while retrieving function: " << error.debugDescription.UTF8String << std::endl;
                return nil;
            }

            return function;
        }

        static id<MTLFunction> GetEntryFunction(id<MTLLibrary> library, MTLFunctionConstantValues* constants) {
            return MetalManager::GetFunction(@"runQueryKernelBackup", library, constants);
        }

        static id<MTLFunction> GetInternalBinaryFunction(NSString* _Nonnull funcName, id<MTLLibrary> library) {
            auto functionDescriptor = [MTLFunctionDescriptor new];
            functionDescriptor.name = funcName;
            functionDescriptor.options = MTLFunctionOptionCompileToBinary;
            NSError* error = nil;
            auto function = [library newFunctionWithDescriptor:functionDescriptor error:&error];
            if (error) {
                std::cerr << "Failed to compile function: " << funcName.UTF8String << " got error: " << error.debugDescription.UTF8String << std::endl;
                return nil;
            }

            return function;
        }

        template<typename ...Args>
        static MTLLinkedFunctions* GetLinkedFunctions(id<MTLLibrary> library, Args... args) {
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

        static id<MTLComputePipelineState> GetComputePipeline(id<MTLLibrary> library, MTLFunctionConstantValues* constants) {
            auto computeDescriptor = [MTLComputePipelineDescriptor new];
            computeDescriptor.supportAddingBinaryFunctions = true;

            auto linkedFunctions = MetalManager::GetLinkedFunctions(library);

            auto function = MetalManager::GetEntryFunction(library, constants);
            if (!function) {
                std::cerr << "Failed to get entry function: " << std::endl;
                return nil;
            }

            computeDescriptor.computeFunction = function;
            computeDescriptor.linkedFunctions = linkedFunctions;

            // A pipeline runs a single function, optionally manipulating the input data before running the function, and the output data afterwards.
            // Creating the pipeline finishes compiling the shader for the GPU.
            NSError* error = nil;
            auto pipeline = [library.device newComputePipelineStateWithDescriptor:computeDescriptor
                                                                          options:MTLPipelineOptionNone
                                                                       reflection:nil
                                                                            error:&error];
            if (error) {
                std::cerr << "Failed while creating pipeline: " << error.description.UTF8String << std::endl;
                return nil;
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

    std::vector<char> SerializeRawTable(const metaldb::reader::RawTable& rawTable) {
        std::vector<char> rawDataSerialized;

        {
            // Write header section
            // size of the header
            using HeaderSizeType = decltype(((metaldb::RawTable*)nullptr)->GetSizeOfHeader());
            HeaderSizeType sizeOfHeader = 0;

            // Allocate space for header size
            for (std::size_t i = 0; i < sizeof(HeaderSizeType); ++i) {
                rawDataSerialized.push_back(0);
            }
            sizeOfHeader += sizeof(HeaderSizeType);

            // Size of data
            // Allocate space for buffer size
            {
                for (std::size_t n = 0; n < sizeof(metaldb::types::SizeType); ++n) {
                    // Read the nth byte
                    rawDataSerialized.push_back((int8_t)(rawTable.data.size() >> (8 * n)) & 0xff);
                }
                sizeOfHeader += sizeof(metaldb::types::SizeType);
            }

            // num rows
            {
                using NumRowsType = decltype(((metaldb::RawTable*)nullptr)->GetNumRows());
                for (std::size_t n = 0; n < sizeof(NumRowsType); ++n) {
                    // Read the nth byte
                    rawDataSerialized.push_back((int8_t)(rawTable.numRows() >> (8 * n)) & 0xff);
                }
                sizeOfHeader += sizeof(NumRowsType);
            }

            // Write the row indexes
            {
                using RowIndexType = decltype(((metaldb::RawTable*)nullptr)->GetRowIndex(0));
                for (const auto& index : rawTable.rowIndexes) {
                    for (std::size_t n = 0; n < sizeof(RowIndexType); ++n) {
                        // Read the nth byte
                        rawDataSerialized.push_back((int8_t)(index >> (8 * n)) & 0xff);
                    }
                }

                sizeOfHeader += rawTable.numRows() * sizeof(RowIndexType);
            }

            // Write in size of header
            for (std::size_t n = 0; n < sizeof(HeaderSizeType); ++n) {
                // Read the nth byte
                rawDataSerialized.at(n) = ((int8_t)(sizeOfHeader >> (8 * n)) & 0xff);
            }
        }

        std::copy(rawTable.data.begin(), rawTable.data.end(), std::back_inserter(rawDataSerialized));
        return rawDataSerialized;
    }

}

auto metaldb::engine::Engine::runImpl(const reader::RawTable& rawTable, instruction_serialized_type&& instructions) -> Dataframe {
    auto manager = MetalManager::Create();
    assert(manager);

    auto rawDataSerialized = SerializeRawTable(rawTable);
    std::array<int8_t, 1'000'000> outputBuffer{0};

    manager->run(rawDataSerialized, instructions, outputBuffer, rawTable.numRows());

    return;

    // Read output buffer to Dataframe.
    Dataframe df;

    // Print to console
    {
        size_t sizeOfHeader = outputBuffer[0];
        size_t numBytes = *(uint16_t*)(&outputBuffer[1]);
        size_t numColumns = outputBuffer[3];

        std::cout << "Size of header: " << sizeOfHeader << std::endl;
        std::cout << "Num bytes: " << numBytes << std::endl;
        std::cout << "Num columns: " << numColumns << std::endl;

        // Allocate this once so we don't change it
        std::vector<std::size_t> columnSizes{numColumns, 0};
        std::vector<ColumnType> columnTypes{numColumns, ColumnType::Unknown};
        std::vector<size_t> variableLengthColumns;
        for (size_t i = 0; i < numColumns; ++i) {
            const auto columnType = (ColumnType) outputBuffer[3 + i];
            columnTypes.at(i) = columnType;
            std::cout << "Column Type: " << i << " " << columnType << std::endl;

            switch (columnType) {
            case String:
                columnSizes.at(i) = 0;
                variableLengthColumns.push_back(i);
                break;
            case Float:
                columnSizes.at(i) = sizeof(metaldb::types::FloatType);
                break;
            case Integer:
                columnSizes.at(i) = sizeof(metaldb::types::IntegerType);
                break;
            case Unknown:
                assert(false);
                break;
            }
        }

        // Read the row
        size_t i = sizeOfHeader;
        while (i < numBytes) {
            // Read the column sizes for the dynamic sized ones
            for (const auto& varLengthCol : variableLengthColumns) {
                columnSizes[varLengthCol] = (std::size_t) outputBuffer[i++];
            }

            // Read the row
            for (int col = 0; col < numColumns; ++col) {
                auto columnType = columnTypes.at(col);
                auto columnSize = columnSizes.at(col);

                switch (columnType) {
                case Integer: {
                    auto* val = (types::IntegerType*) &outputBuffer[i];
                    std::cout << "Reading Int: " << *val << " with size: " << columnSize << std::endl;
                    break;
                }
                case Float: {
                    auto* val = (types::FloatType*) &outputBuffer[i];
                    std::cout << "Reading Float: " << *val << " with size: " << columnSize << std::endl;
                    break;
                }
                case String: {
                    std::string temp;
                    for (std::size_t j = 0; j < columnSize; ++j) {
                        temp += outputBuffer[i + j];
                    }

                    std::cout << "Reading String: " << temp << " with size: " << columnSize << std::endl;
                    break;
                }
                case Unknown:
                    std::cout << "Unsupported Column" << std::endl;
                    break;
                }

                i += columnSize;
            }
        }
    }

    return df;
}
