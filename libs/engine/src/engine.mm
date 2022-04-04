#include <metaldb/engine/Engine.hpp>

#import <MetalKit/MetalKit.h>

#include "engine.h"

#include <memory>
#include <iostream>
#include <array>

namespace {
    class MetalManager {
    public:
        static std::unique_ptr<MetalManager> Create() {
            MetalManager manager;
            manager.constants = [[MTLFunctionConstantValues alloc] init];

            manager.device = GetDevice();
            if (!manager.device) {
                std::cerr << "Failed to get default device" << std::endl;
                return nullptr;
            }

            manager.library = GetLibrary(manager.device);
            if (!manager.library) {
                return nullptr;
            }

            manager.pipeline = GetComputePipeline(manager.library, manager.constants);
            if (!manager.pipeline) {
                return nullptr;
            }

            return std::make_unique<MetalManager>(std::move(manager));
        }

        id<MTLDevice> device;
        id<MTLLibrary> library;
        id<MTLComputePipelineState> pipeline;

        MTLFunctionConstantValues* constants;

    private:
        static id<MTLDevice> GetDevice() {
            for (id<MTLDevice> device : MTLCopyAllDevices()) {
                if (device.isLowPower) {
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

        static id<MTLFunction> GetFunction(id<MTLLibrary> library, MTLFunctionConstantValues* constants) {
            NSError* error = nil;
            auto function = [library newFunctionWithName:@"runQueryKernel" constantValues:constants error:&error];
            if (error) {
                std::cerr << "Failed while retrieving function: " << error.debugDescription.UTF8String << std::endl;
                return nil;
            }

            return function;
        }

        static id<MTLComputePipelineState> GetComputePipeline(id<MTLLibrary> library, MTLFunctionConstantValues* constants) {
            auto function = GetFunction(library, constants);
            if (!function) {
                return nil;
            }

            // A pipeline runs a single function, optionally manipulating the input data before running the function, and the output data afterwards.
            // Creating the pipeline finishes compiling the shader for the GPU.
            NSError* error = nil;
            auto pipeline = [library.device newComputePipelineStateWithFunction:function error:&error];
            if (error) {
                std::cerr << "Failed while creating pipeline: " << error.debugDescription.UTF8String << std::endl;
                return nil;
            }

            return pipeline;
        }
    };

    std::vector<char> SerializeRawTable(const metaldb::reader::RawTable& rawTable) {
        std::vector<char> rawDataSerialized;

        {
            // TODO: Could use multiple bytes for counts
            // Write header section
            // size of the header
            rawDataSerialized.push_back(1);

            // Size of data
            rawDataSerialized.push_back(rawTable.data.size());
            rawDataSerialized.at(0)++;

            // num rows
            rawDataSerialized.push_back(rawTable.numRows());
            rawDataSerialized.at(0)++;

            // Write the row indexes
            for (const auto& index : rawTable.rowIndexes) {
                rawDataSerialized.push_back(index);
            }
            rawDataSerialized.at(0) += rawTable.numRows();
        }

        std::copy(rawTable.data.begin(), rawTable.data.end(), std::back_inserter(rawDataSerialized));
        return rawDataSerialized;
    }

}

auto metaldb::engine::Engine::runImpl(const reader::RawTable& rawTable, instruction_serialized_type&& instructions) -> Dataframe {
//    auto manager = MetalManager::Create();
//    assert(manager);
    size_t numRows = 1000;
    auto rawDataSerialized = SerializeRawTable(rawTable);
    std::array<char, 1'000'000> outputBuffer;
    for (uint i = 0; i < numRows; ++i) {
        runQueryKernelImpl(rawDataSerialized.data(), instructions.data(), outputBuffer.data(), i);
    }

    // Read output buffer to Dataframe.
    Dataframe df;

    // Print to console
    {
        size_t sizeOfHeader = outputBuffer[0];
        size_t numColumns = outputBuffer[1];

        std::cout << "Size of header: " << sizeOfHeader << std::endl;
        std::cout << "Num columns: " << numColumns << std::endl;

        std::vector<ColumnType> columnTypes{numColumns};
        for (size_t i = 0; i < numColumns; ++i) {
            columnTypes.at(i) = (ColumnType) outputBuffer[2 + i];
            std::cout << "Column Type: " << i << " " << columnTypes.at(i) << std::endl;
        }

        // Read the row
        for (size_t i = sizeOfHeader; i < outputBuffer.size(); ++i) {

        }
    }

    return df;
}
