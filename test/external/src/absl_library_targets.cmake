#
# test/external/CMakeLists.txt
#
#
# The MIT License
#
# Copyright (c) 2022 TileDB, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# basic list obtained from an installed abslTargets.cmake foreach(expectedTarget ...)
# at the time this was created, abseil build process created absl:: namespaced target
# for each individual library built.
#
# update list as needed if changes in abseil require

set(TILEDB_ABSL_LIBRARY_TARGETS
  absl::algorithm
  absl::algorithm_container
  absl::any
  absl::atomic_hook
  absl::bad_any_cast
  absl::bad_any_cast_impl
  absl::bad_optional_access
  absl::bad_variant_access
  absl::base
  absl::base_internal
  absl::bind_front
  absl::bits
  absl::btree
  absl::city
  absl::civil_time
  absl::cleanup
  absl::cleanup_internal
  absl::compare
  absl::compressed_tuple
  absl::config
  absl::container_common
  absl::container_memory
  absl::cord
  absl::cord_internal
  absl::cordz_functions
  absl::cordz_handle
  absl::cordz_info
  absl::cordz_sample_token
  absl::cordz_statistics
  absl::cordz_update_scope
  absl::cordz_update_tracker
  absl::core_headers
  absl::counting_allocator
  absl::debugging
  absl::debugging_internal
  absl::demangle_internal
  absl::dynamic_annotations
  absl::endian
  absl::errno_saver
  absl::examine_stack
  absl::exponential_biased
  absl::failure_signal_handler
  absl::fast_type_id
  absl::fixed_array
  absl::flags
  absl::flags_commandlineflag
  absl::flags_commandlineflag_internal
  absl::flags_config
  absl::flags_internal
  absl::flags_marshalling
  absl::flags_parse
  absl::flags_path_util
  absl::flags_private_handle_accessor
  absl::flags_program_name
  absl::flags_reflection
  absl::flags_usage
  absl::flags_usage_internal
  absl::flat_hash_map
  absl::flat_hash_set
  absl::function_ref
  absl::graphcycles_internal
  absl::hash
  absl::hash_function_defaults
  absl::hash_policy_traits
  absl::hashtable_debug
  absl::hashtable_debug_hooks
  absl::hashtablez_sampler
  absl::have_sse
  absl::inlined_vector
  absl::inlined_vector_internal
  absl::int128
  absl::kernel_timeout_internal
  absl::layout
  absl::leak_check
  absl::leak_check_disable
  absl::log_severity
  absl::low_level_hash
  absl::malloc_internal
  absl::memory
  absl::meta
  absl::node_hash_map
  absl::node_hash_policy
  absl::node_hash_set
  absl::numeric
  absl::numeric_representation
  absl::optional
  absl::periodic_sampler
  absl::pretty_function
  absl::random_bit_gen_ref
  absl::random_distributions
  absl::random_internal_distribution_caller
  absl::random_internal_distribution_test_util
  absl::random_internal_fast_uniform_bits
  absl::random_internal_fastmath
  absl::random_internal_generate_real
  absl::random_internal_iostream_state_saver
  absl::random_internal_mock_helpers
  absl::random_internal_nonsecure_base
  absl::random_internal_pcg_engine
  absl::random_internal_platform
  absl::random_internal_pool_urbg
  absl::random_internal_randen
  absl::random_internal_randen_engine
  absl::random_internal_randen_hwaes
  absl::random_internal_randen_hwaes_impl
  absl::random_internal_randen_slow
  absl::random_internal_salted_seed_seq
  absl::random_internal_seed_material
  absl::random_internal_traits
  absl::random_internal_uniform_helper
  absl::random_internal_wide_multiply
  absl::random_random
  absl::random_seed_gen_exception
  absl::random_seed_sequences
  absl::raw_hash_map
  absl::raw_hash_set
  absl::raw_logging_internal
  absl::sample_recorder
  absl::scoped_set_env
  absl::span
  absl::spinlock_wait
  absl::stacktrace
  absl::status
  absl::statusor
  absl::str_format
  absl::str_format_internal
  absl::strerror
  absl::strings
  absl::strings_internal
  absl::symbolize
  absl::synchronization
  absl::throw_delegate
  absl::time
  absl::time_zone
  absl::type_traits
  absl::utility
  absl::variant
)
