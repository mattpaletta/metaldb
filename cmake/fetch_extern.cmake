include(FetchContent)

function(fetch_extern name repo tag)
        FetchContent_Declare(
                ${name}
                GIT_REPOSITORY ${repo}
                GIT_TAG ${tag}
        )
        FetchContent_GetProperties(${name})
	if (NOT ${name}_POPULATED)
	        FetchContent_Populate(${name})
	endif()
	set(source_dir "${${name}_SOURCE_DIR}")
	set(binary_dir "${${name}_BINARY_DIR}")
	if (EXISTS ${source_dir}/CMakeLists.txt)
		add_subdirectory(${source_dir} ${binary_dir} EXCLUDE_FROM_ALL)
	endif()

	# Store source & binary dir as global variables
	set_property(GLOBAL PROPERTY ${name}_SOURCE_DIR ${source_dir})
	set_property(GLOBAL PROPERTY ${name}_BINARY_DIR ${binary_dir})
endfunction(fetch_extern)

function(pseudo_fetch_url url dest filename)
	message("Fetching: ${url} to ${dest}/${filename}")
	if (NOT EXISTS ${dest}/${filename})
		message(STATUS "Did not find: ${dest}/${filename} trying to download")
		find_program(WGET wget QUIET)
		if (NOT ${WGET} STREQUAL "WGET_NOT-FOUND")
			message("Downloading: " ${url})
			execute_process(COMMAND ${WGET} -q ${url} -P ${dest})
		else()
			message(FATAL_ERROR "Failed to find file: ${dest}/${filename} please run: (${WGET} -q ${url} -P ${dest})")
		endif()
	else()
		message("Found ${dest}/${filename}")
	endif()
endfunction(pseudo_fetch_url)
