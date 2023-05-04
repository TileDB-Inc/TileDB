#
# Interface library for multithreading support
#
add_library(flat_mt INTERFACE)
target_compile_definitions(flat_mt INTERFACE EXECUTION_POLICY)

find_package(Threads REQUIRED)
target_link_libraries(flat_mt INTERFACE Threads::Threads)

# find_package(TBB REQUIRED)
# target_link_libraries(flat_mt INTERFACE TBB::tbb)

# find_package(OpenMP REQUIRED)
# include(FindOpenMP)
# target_link_libraries(flat_mt INTERFACE OpenMP::OpenMP_CXX)
