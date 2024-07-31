/*
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 */

/**
 * NOTE: The values of these enums are serialized to the array schema and/or
 * fragment metadata. Therefore, the values below should never change,
 * otherwise backwards compatibility breaks.
 */

// clang-format is disabled on the first enum so that we can manually indent it
// properly.

// clang-format off
#ifdef TILEDB_LAYOUT_ENUM
    /** Row-major layout */
    TILEDB_LAYOUT_ENUM(ROW_MAJOR) = 0,
    /** Column-major layout */
    TILEDB_LAYOUT_ENUM(COL_MAJOR) = 1,
    /** Global-order layout */
    TILEDB_LAYOUT_ENUM(GLOBAL_ORDER) = 2,
    /** Unordered layout */
    TILEDB_LAYOUT_ENUM(UNORDERED) = 3,
    /** Hilbert layout */
    TILEDB_LAYOUT_ENUM(HILBERT) = 4,
#endif
    // clang-format on
