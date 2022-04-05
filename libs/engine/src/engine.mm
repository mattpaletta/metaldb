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
    std::array<int8_t, 1'000'000> outputBuffer{0};
    for (uint i = 0; i < numRows; ++i) {
        runQueryKernelImpl(rawDataSerialized.data(), instructions.data(), outputBuffer.data(), i);
    }

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
                }

                i += columnSize;
            }
        }
    }

    return df;
}
