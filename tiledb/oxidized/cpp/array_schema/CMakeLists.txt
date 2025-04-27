include(unit_test)

commence(unit_test enumeration_rs)
    this_target_sources(
        main.cc
        enumeration.cc
    )
    this_target_object_libraries(enumeration enumeration_rs mem_helpers)
conclude(unit_test)
