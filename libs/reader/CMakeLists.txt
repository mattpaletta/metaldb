file(GLOB_RECURSE INC_FILES include/*.hpp include/*.h)
file(GLOB_RECURSE SRC_FILES src/*.cpp src/*.c)

add_library(metaldb_reader ${SRC_FILES} ${INC_FILES})
target_include_directories(metaldb_reader PUBLIC include)
target_link_libraries(metaldb_reader PRIVATE cppnotstdlib)
