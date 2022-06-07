file(GLOB_RECURSE INC_FILES include/*.hpp include/*.h)
file(GLOB_RECURSE SRC_FILES src/*.cpp src/*.c src/*.m src/*.mm src/*.h src/*.hpp)

if (NOT TARGET taskflow)
    set(TF_BUILD_BENCHMARKS OFF)
    set(TF_BUILD_CUDA OFF)
    set(TF_BUILD_SYCL OFF)
	set(TF_BUILD_TESTS OFF)
	set(TF_BUILD_EXAMPLES OFF)
    set(TF_BUILD_PROFILER OFF)
    fetch_extern(taskflow https://github.com/taskflow/taskflow v3.3.0)
endif()

find_library(METAL_LIBRARY Metal)
find_library(METALKIT_LIBRARY MetalKit)
find_library(CORE_FOUNDATION_LIBARY Foundation)

add_library(metaldb_engine_internal INTERFACE)
target_include_directories(metaldb_engine_internal INTERFACE src)
target_link_libraries(metaldb_engine_internal INTERFACE metaldb_reader metaldb_query_engine Taskflow)

add_library(metaldb_engine STATIC ${SRC_FILES} ${INC_FILES})
target_include_directories(metaldb_engine PUBLIC include)
target_link_libraries(metaldb_engine PRIVATE metaldb_engine_internal ${METAL_LIBRARY} ${METALKIT_LIBRARY} ${CORE_FOUNDATION_LIBARY})
add_dependencies(metaldb_engine metaldb_engine_shaders_lib)

source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${INC_FILES} ${SRC_FILES})

# temporarily for debugging
target_link_libraries(metaldb_engine PUBLIC metaldb_engine_shaders)

file(GLOB_RECURSE TEST_SRC_FILES test/*.cpp)
cpptest_configure_target(
    TARGET metaldb_engine
    TEST_SRC_FILES ${TEST_SRC_FILES})
target_link_libraries(metaldb_engineTests PRIVATE metaldb_engine_internal)
