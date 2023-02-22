# credit: https://cliutils.gitlab.io/modern-cmake/chapters/projects/submodule.html

find_package(Git QUIET)

# NOTE: cannot use PROJECT_SOURCE_DIR here because we need to run this *before*
#       `project()` is called in order to ensure vcpkg is available.

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
    # Update submodules as needed
    if(_TILEDB_CMAKE_INIT_GIT_SUBMODULES)
        message(STATUS "Update submodules")
        # TODO check pinned version here?
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT
                        ECHO_OUTPUT_VARIABLE
                        COMMAND_ECHO STDOUT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/external/vcpkg/LICENSE.txt")
    message(FATAL_ERROR "The submodules were not downloaded! _TILEDB_CMAKE_INIT_GIT_SUBMODULES"
                        " was turned off or failed. Please update submodules and try again.")
endif()
