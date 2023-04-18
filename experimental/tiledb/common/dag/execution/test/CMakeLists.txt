#
# experimental/tiledb/common/dag/execution/test/CMakeLists.txt
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

#
# Define unit tests.  Note that tests for frugal, throw_catch, and bountiful are
# part of unit_scheduler.  The tests for the duffs scheduler should be
# migrated to unit_scheduler at some point (and unit_scheduler should be
# cleaned up per some of the practices in unit_duffs).
#
dag_add_header_only_unit_test(scheduler)
dag_add_header_only_unit_test(duffs)
dag_add_header_only_unit_test(threadpool)
dag_add_header_only_unit_test(tasks)
dag_add_header_only_unit_test(task_state_machine)
dag_add_header_only_unit_test(scheduler_sieve)

# The unit test for frugal needs to be brought up date
# dag_add_header_only_unit_test(frugal)
