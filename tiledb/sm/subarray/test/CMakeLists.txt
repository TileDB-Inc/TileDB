find_package(Catch_EP REQUIRED)

add_executable(unit_range_subset EXCLUDE_FROM_ALL)
target_link_libraries(unit_range_subset
    PRIVATE range_subset
    PUBLIC Catch2::Catch2WithMain
)
target_sources(unit_range_subset PUBLIC main.cc unit_range_subset.cc)

add_test(
    NAME "unit_range_subset"
    COMMAND $<TARGET_FILE:unit_range_subset>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
add_dependencies(check unit_range_subset)

add_executable(unit_add_ranges_list EXCLUDE_FROM_ALL)
target_sources(unit_add_ranges_list PUBLIC main.cc unit_add_ranges_list.cc)

target_link_libraries(unit_add_ranges_list
    PUBLIC Catch2::Catch2
)
target_link_libraries(unit_add_ranges_list PUBLIC $<TARGET_OBJECTS:TILEDB_CORE_OBJECTS>)
target_link_libraries(unit_add_ranges_list PUBLIC tiledb_test_support_lib)

target_include_directories(unit_add_ranges_list
  PRIVATE "$<TARGET_PROPERTY:TILEDB_CORE_OBJECTS,INCLUDE_DIRECTORIES>"
)

# We want tests to continue as normal even as the API is changing,
# so don't warn for deprecations, since they'll be escalated to errors.
if (NOT MSVC)
  target_compile_options(unit_add_ranges_list PRIVATE -Wno-deprecated-declarations)
endif()

add_test(
    NAME "unit_add_ranges_list"
    COMMAND $<TARGET_FILE:unit_add_ranges_list>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
add_dependencies(check unit_add_ranges_list)
