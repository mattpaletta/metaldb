#import "MetalManager.hpp"

auto metaldb::MetalManager::Create() noexcept -> std::shared_ptr<MetalManager> {
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

auto metaldb::MetalManager::MaxNumRows() const noexcept -> std::size_t {
    return this->pipeline.maxTotalThreadsPerThreadgroup;
}

auto metaldb::MetalManager::MaxMemory() const noexcept -> std::size_t {
    return this->pipeline.staticThreadgroupMemoryLength;
}

void metaldb::MetalManager::runCPU(const std::vector<char>& serializedData, const std::vector<metaldb::InstSerializedValue>& instructions, OutputBufferType& outputBuffer, size_t numRows) noexcept {
    // Mimic the GPU on the CPU
    // Force const cast the pointers (bad)
    const auto numInstructions = instructions.at(0);
    std::array<metaldb::OutputRow::NumBytesType, metaldb::DbConstants::MAX_NUM_ROWS> scratch;
    metaldb::RawTable rawTable((char*) serializedData.data());
    metaldb::DbConstants constants{rawTable, outputBuffer.data(), scratch.data()};

    constants.threadgroup_position_in_grid = 0;
    constants.thread_execution_width = 1;
    for (std::size_t thread = 0; thread < numRows; ++thread) {
        constants.thread_position_in_grid = thread;
        constants.thread_position_in_threadgroup = thread;
        metaldb::runInstructions((metaldb::InstSerializedValue*) &instructions.at(1), numInstructions, constants);
    }
}

void metaldb::MetalManager::run(const std::vector<char>& serializedData, const std::vector<metaldb::InstSerializedValue>& instructions, OutputBufferType& outputBuffer, size_t numRows) noexcept {
    if (numRows == 0) {
        return;
    }
    //        return this->runCPU(serializedData, instructions, outputBuffer, numRows);

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
        ((metaldb::InstSerializedValue*)inputBuffer.contents)[i] = serializedData.at(i);
    }

    for (std::size_t i = 0; i < instructions.size(); ++i) {
        ((OutputBufferType::value_type*)instructionsBuffer.contents)[i] = instructions.at(i);
    }

    //        auto captureManager = [MTLCaptureManager sharedCaptureManager];
    //        auto captureDescriptor = [MTLCaptureDescriptor new];
    //        captureDescriptor.captureObject = this->device;
    //
    //        NSError* error = nil;
    //        [captureManager startCaptureWithDescriptor:captureDescriptor error:&error];
    //        if (error) {
    //            std::cerr << "Failed to start capture: " << error.debugDescription.UTF8String << std::endl;
    //        }

    auto commandBuffer = [this->commandQueue commandBuffer];
    NSUInteger threadGroupSize = std::min(this->MaxNumRows(), numRows);

    MTLSize gridSize = MTLSizeMake(threadGroupSize, 1, 1);

    std::cout << "Executing with Grid Size (" << gridSize.width << "," << gridSize.height << "," << gridSize.depth << ")" << std::endl;

    {
        // Send commands to encoder
        id <MTLComputeCommandEncoder> computeEncoder = [commandBuffer computeCommandEncoder];

        [computeEncoder setComputePipelineState:this->pipeline];

        [computeEncoder setBuffer:inputBuffer offset:0 atIndex:0];
        [computeEncoder setBuffer:instructionsBuffer offset:0 atIndex:1];
        [computeEncoder setBuffer:outputBufferMtl offset:0 atIndex:2];

        [computeEncoder dispatchThreadgroups:MTLSizeMake(1, 1, 1) threadsPerThreadgroup:gridSize];
        [computeEncoder endEncoding];
    }

    [commandBuffer commit];
    [commandBuffer waitUntilCompleted];

    auto* contents = (int8_t*) outputBufferMtl.contents;
    for (std::size_t i = 0; i < outputBuffer.size(); ++i) {
        outputBuffer.at(i) = contents[i];
    }
}

auto metaldb::MetalManager::GetDevice() noexcept -> id<MTLDevice> _Nullable {
    auto check = [](id<MTLDevice> device) {
        return !device.isLowPower;
    };

    for (id<MTLDevice> device : MTLCopyAllDevices()) {
        if (check(device)) {
            return device;
        }
    }

    return MTLCreateSystemDefaultDevice();
}

auto metaldb::MetalManager::GetLibrary(id<MTLDevice> _Nonnull device) noexcept -> id<MTLLibrary> _Nullable {
    NSError* error = nullptr;
    auto library = [device newLibraryWithFile:@"../libs/engine_shaders/MetalDbEngine.metallib" error:&error];

    if (error) {
        std::cerr << "Failed while retrieving library: " << error.debugDescription.UTF8String << std::endl;
        return nullptr;
    }

    return library;
}

auto metaldb::MetalManager::GetFunction(NSString* _Nonnull funcName, id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) noexcept -> id<MTLFunction> _Nullable {
    NSError* error = nullptr;
    auto function = [library newFunctionWithName:funcName constantValues:constants error:&error];
    if (error) {
        std::cerr << "Failed while retrieving function: " << error.debugDescription.UTF8String << std::endl;
        return nullptr;
    }

    return function;
}

auto metaldb::MetalManager::GetEntryFunction(id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) noexcept -> id<MTLFunction> _Nullable {
    return MetalManager::GetFunction(@"runQueryKernel", library, constants);
}

auto metaldb::MetalManager::GetInternalBinaryFunction(NSString* _Nonnull funcName, id<MTLLibrary> _Nonnull library) noexcept -> id<MTLFunction> _Nullable {
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

auto metaldb::MetalManager::GetComputePipeline(id<MTLLibrary> _Nonnull library, MTLFunctionConstantValues* _Nonnull constants) noexcept -> id<MTLComputePipelineState> _Nullable {
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
