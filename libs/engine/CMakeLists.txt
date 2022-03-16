file(GLOB_RECURSE INC_FILES include/*.hpp include/*.h)
file(GLOB_RECURSE SRC_FILES src/*.cpp src/*.c src/*.m src/*.mm)

add_library(metaldb_engine ${SRC_FILES} ${INC_FILES})
target_include_directories(metaldb_engine PUBLIC include)
target_link_libraries(metaldb_engine PRIVATE metaldb_reader)
target_link_libraries(metaldb_engine PUBLIC gpgpu)
add_dependencies(metaldb_engine metaldb_engine_shaders_lib)

# temporarily for debugging
target_link_libraries(metaldb_engine PRIVATE metaldb_engine_shaders)
