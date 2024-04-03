cmake_minimum_required(VERSION 3.12)

project(libfaketime VERSION 0.9.10)

add_library(
        ${PROJECT_NAME}
        src/libfaketime.c
)

target_link_libraries(
        ${PROJECT_NAME}
        rt
)

target_include_directories(${PROJECT_NAME}
        PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>"
        "$<INSTALL_INTERFACE:include>"
)

file(GLOB_RECURSE PUBLIC_HEADERS "src/*.h")

target_sources(
        ${PROJECT_NAME}
        PUBLIC
        FILE_SET HEADERS
        BASE_DIRS src
        FILES ${PUBLIC_HEADERS}
)

install(TARGETS ${PROJECT_NAME}
        EXPORT ${PROJECT_NAME}
        FILE_SET HEADERS
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(EXPORT ${PROJECT_NAME}
        FILE unofficial-${PROJECT_NAME}-targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/unofficial-${PROJECT_NAME}
        NAMESPACE unofficial::libfaketime::
)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

configure_package_config_file(
        unofficial-${PROJECT_NAME}-config.cmake.in
        unofficial-${PROJECT_NAME}-config.cmake
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/unofficial-${PROJECT_NAME})

write_basic_package_version_file(
        unofficial-${PROJECT_NAME}-config-version.cmake
        VERSION ${CMAKE_PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion)

install(FILES
        ${CMAKE_BINARY_DIR}/unofficial-${PROJECT_NAME}-config.cmake
        ${CMAKE_BINARY_DIR}/unofficial-${PROJECT_NAME}-config-version.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/unofficial-${PROJECT_NAME})
