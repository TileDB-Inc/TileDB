#
# Interface library for BLAS support
#
add_library(flat_blas INTERFACE)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    # include macOS-specific flags for native BLAS
    # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -framework Accelerate")
    target_compile_options(flat_blas INTERFACE -framework Accelerate)
    target_link_options(flat_blas INTERFACE -framework Accelerate)
else()
    # include flags for other operating systems BLAS
    # find_package(MKL CONFIG REQUIRED)
    find_package(MKL REQUIRED)
    include_directories(${MKL_INCLUDE_DIRS})
    # target_link_libraries(${PROJECT_NAME} ${MKL_LIBRARIES})

    target_compile_options(flat_blas INTERFACE $<TARGET_PROPERTY:MKL::MKL,INTERFACE_COMPILE_OPTIONS>)
    target_include_directories(flat_blas INTERFACE $<TARGET_PROPERTY:MKL::MKL,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(flat_blas INTERFACE $<LINK_ONLY:MKL::MKL>)

endif()
