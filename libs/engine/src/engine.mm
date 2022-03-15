#include <metaldb/engine/Engine.hpp>

#import <MetalKit/MetalKit.h>

auto metaldb::engine::Engine::runImpl(const reader::RawTable& rawTable, instruction_serialized_type&& instructions) -> Dataframe {
    auto device = MTLCreateSystemDefaultDevice();
    NSError* error = nil;
    auto library = [device newLibraryWithFile:@"../libs/engine_shaders/MetalDbEngine.metallib" error:&error];
    assert(library);
}
