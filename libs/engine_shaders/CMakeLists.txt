file(GLOB_RECURSE METAL_FILES *.metal)
file(GLOB_RECURSE INC_FILES *.h *.hpp *.metalh)

add_library(metaldb_engine_shaders INTERFACE ${INC_FILES})
target_include_directories(metaldb_engine_shaders INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
set(metaldb_engine_shaders_DEPENDS "")
foreach(shader IN LISTS METAL_FILES)
	message("Found File: ${shader}")
	get_filename_component(shader_name ${shader} NAME_WLE)
	if (NOT TARGET "${shader_name}_shader_compile")
		add_custom_target("${shader_name}_shader_compile"
			COMMAND xcrun -sdk macosx metal -c ${shader} -o ${shader_name}.air
			DEPENDS ${shader}
			WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
			COMMENT "Compiling Shader: ${shader_name}")
		add_dependencies(metaldb_engine_shaders "${shader_name}_shader_compile")
		list(APPEND metaldb_engine_shaders_DEPENDS "${shader_name}.air")
	endif()
endforeach()

add_custom_target("metaldb_engine_shaders_lib"
	COMMAND xcrun -sdk macosx metallib -o MetalDbEngine.metallib ${metaldb_engine_shaders_DEPENDS}
	WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
	COMMENT "Linking Metal Lib")
add_dependencies(metaldb_engine_shaders_lib metaldb_engine_shaders)
