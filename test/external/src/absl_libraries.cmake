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
# basic list obtained from 'dir /b <binary_build_dir>/install/lib/*absl*lib'
# at the time this was created, abseil build process created absl:: namespaced target
# for each individual library built.
#
# update list as needed if changes in abseil require

set(TILEDB_ABSL_LIBRARIES
    absl::absl_bad_any_cast_impl.lib
    absl::absl_bad_optional_access.lib
    absl::absl_bad_variant_access.lib
    absl::absl_base.lib
    absl::absl_city.lib
    absl::absl_civil_time.lib
    absl::absl_cord.lib
    absl::absl_debugging_internal.lib
    absl::absl_demangle_internal.lib
    absl::absl_dynamic_annotations.lib
    absl::absl_examine_stack.lib
    absl::absl_exponential_biased.lib
    absl::absl_failure_signal_handler.lib
    absl::absl_flags.lib
    absl::absl_flags_config.lib
    absl::absl_flags_internal.lib
    absl::absl_flags_marshalling.lib
    absl::absl_flags_parse.lib
    absl::absl_flags_program_name.lib
    absl::absl_flags_registry.lib
    absl::absl_flags_usage.lib
    absl::absl_flags_usage_internal.lib
    absl::absl_graphcycles_internal.lib
    absl::absl_hash.lib
    absl::absl_hashtablez_sampler.lib
    absl::absl_int128.lib
    absl::absl_leak_check.lib
    absl::absl_leak_check_disable.lib
    absl::absl_log_severity.lib
    absl::absl_malloc_internal.lib
    absl::absl_periodic_sampler.lib
    absl::absl_random_distributions.lib
    absl::absl_random_internal_distribution_test_util.lib
    absl::absl_random_internal_pool_urbg.lib
    absl::absl_random_internal_randen.lib
    absl::absl_random_internal_randen_hwaes.lib
    absl::absl_random_internal_randen_hwaes_impl.lib
    absl::absl_random_internal_randen_slow.lib
    absl::absl_random_internal_seed_material.lib
    absl::absl_random_seed_gen_exception.lib
    absl::absl_random_seed_sequences.lib
    absl::absl_raw_hash_set.lib
    absl::absl_raw_logging_internal.lib
    absl::absl_scoped_set_env.lib
    absl::absl_spinlock_wait.lib
    absl::absl_stacktrace.lib
    absl::absl_status.lib
    absl::absl_strings.lib
    absl::absl_strings_internal.lib
    absl::absl_str_format_internal.lib
    absl::absl_symbolize.lib
    absl::absl_synchronization.lib
    absl::absl_throw_delegate.lib
    absl::absl_time.lib
    absl::absl_time_zone.lib
)
