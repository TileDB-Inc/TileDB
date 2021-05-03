/**
 * @file initialize-shuffle.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Initializes `host_implementation` by using C++ static initialization,
 * avoiding the use of pthreads for the same purpose.
 */

/*
 * All the declarations in this block are copied from or derived from
 * shuffle.c. These are a substitute for including an alternate-reality
 * shuffle.h where initialization of host_implementation did not occur
 * entirely within that file.
 */
extern "C"
{
  void set_host_implementation(void);
}
/*
 * Trickery afoot. We need to use C++ static initialization, but we need to run
 * an external initialization function. We accomplish by initializing a dummy
 * static variable with an initialization function that has the side-effect we
 * need.
 */
static short dummy_initializer() noexcept
{
  set_host_implementation();
  return 0;
}
static const auto dummy_variable = dummy_initializer();
