include(unit_test)

commence(unit_test filter_pipeline_rs)
    this_target_sources(
        main.cc
        filter.cc
        filter_pipeline.cc
    )
    this_target_object_libraries(
        filter_rs
        filter_pipeline
        filter_pipeline_rs
        filter_pipeline_test_support
        mem_helpers
    )
conclude(unit_test)
