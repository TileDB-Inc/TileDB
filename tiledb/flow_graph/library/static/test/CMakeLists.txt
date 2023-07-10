#
# flow_graph/library/static/test/CMakeLists.txt
#
# The MIT License
#
# Copyright (c) 2023 TileDB, Inc.
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
# @section Description
#
# The dummy library consists of flow graph elements that don't do anything.
# * Coroutines start off in the "completed" state.
# * Output ports never produce any data.
# * Input ports never consume any data nor ask for data.
# * Edges don't move any data.
#
# In every other way the elements are well behaved. This library is targeted for
# testing, principally the construction of specification graphs. A graph
# composed entirely of dummies should immediately terminate when executed.
#
include(unit_test)
commence(unit_test fgl_static)
  this_target_sources(main.cc test_dummy.cc)
conclude(unit_test)
