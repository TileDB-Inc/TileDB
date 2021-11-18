include(common NO_POLICY_SCOPE)

#
# Object library for other units to depend upon
#
add_library(buffer OBJECT buffer.cc)
target_link_libraries(buffer PUBLIC baseline $<TARGET_OBJECTS:baseline>)

#
# Test-compile of object library ensures link-completeness
#
add_executable(compile_buffer EXCLUDE_FROM_ALL)
target_link_libraries(compile_buffer PRIVATE buffer)
target_sources(compile_buffer PRIVATE
    test/compile_buffer_main.cc $<TARGET_OBJECTS:buffer>
)