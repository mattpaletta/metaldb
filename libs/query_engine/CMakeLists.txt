file(GLOB_RECURSE INC_FILES include/*.hpp include/*.h)
file(GLOB_RECURSE SRC_FILES src/*.cpp src/*.c src/*.m src/*.mm)

add_library(metaldb_query_engine ${SRC_FILES} ${INC_FILES})
target_link_libraries(metaldb_query_engine PUBLIC metaldb_engine_shaders)
target_include_directories(metaldb_query_engine PUBLIC include)
source_group(TREE ${CMAKE_CURRENT_SOURCE_DIR} FILES ${INC_FILES} ${SRC_FILES})
