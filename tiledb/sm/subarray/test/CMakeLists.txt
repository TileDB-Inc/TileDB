find_package(Catch_EP REQUIRED)

add_executable(unit_range_multi_subset EXCLUDE_FROM_ALL)
target_link_libraries(unit_range_multi_subset
    PRIVATE range_multi_subset
    PUBLIC Catch2::Catch2
)
target_sources(unit_range_multi_subset PUBLIC main.cc unit_range_multi_subset.cc)

add_test(
    NAME "unit_range_multi_subset"
    COMMAND $<TARGET_FILE:unit_range_multi_subset>
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
