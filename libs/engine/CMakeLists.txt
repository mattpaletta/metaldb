file(GLOB_RECURSE INC_FILES include/*.hpp include/*.h)
file(GLOB_RECURSE SRC_FILES src/*.cpp src/*.c src/*.m src/*.mm)

find_library(METAL_LIBRARY Metal)
find_library(METALKIT_LIBRARY MetalKit)
find_library(CORE_FOUNDATION_LIBARY Foundation)

add_library(metaldb_engine ${SRC_FILES} ${INC_FILES})
target_include_directories(metaldb_engine PUBLIC include)
target_link_libraries(metaldb_engine PRIVATE metaldb_reader)
target_link_libraries(metaldb_engine PRIVATE ${METAL_LIBRARY} ${METALKIT_LIBRARY} ${CORE_FOUNDATION_LIBARY})
add_dependencies(metaldb_engine metaldb_engine_shaders_lib)

# temporarily for debugging
target_link_libraries(metaldb_engine PUBLIC metaldb_engine_shaders)
