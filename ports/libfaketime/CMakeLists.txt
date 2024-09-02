cmake_minimum_required(VERSION 3.20)

project(faketime VERSION 0.9.10)

add_library(
        ${PROJECT_NAME}
        src/libfaketime.c
)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(
            ${PROJECT_NAME}
            rt
            m
    )
endif()

target_include_directories(${PROJECT_NAME}
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

file(GLOB_RECURSE PUBLIC_HEADERS "src/*.h")

install(FILES ${PUBLIC_HEADERS} DESTINATION include)

install(TARGETS ${PROJECT_NAME}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
