/**
 * @file   work_arounds.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file contains one-off work arounds for external issues.
 */

// This is a work-around for a gcc bug that was fixed in v6.0:
// https://bugs.launchpad.net/ubuntu/+source/gcc-5/+bug/1568899
//
// We've also seen this issue on gcc v7.
//
// The C++ AWS SDK v1.7 introduced this bug into our build because it
// references the hidden gcc symbol '__cpu_model'. To fix this, we
// redefine it here with public visibility.
//
// The struct may not need to strictly match the definition in the gcc
// library, but we're matching it here to be safe.
//
// For reference, the gcc definition can be found here:
// https://github.com/gcc-mirror/gcc/blob/d353bf189d2bbaf4059f402ee4d2a5ea074c349f/libgcc/config/i386/cpuinfo.c
#if __GNUC__ > 0
__attribute__((visibility("default"))) struct __processor_model {
  unsigned int __cpu_vendor;
  unsigned int __cpu_type;
  unsigned int __cpu_subtype;
  unsigned int __cpu_features[1];
} __cpu_model;
#endif
