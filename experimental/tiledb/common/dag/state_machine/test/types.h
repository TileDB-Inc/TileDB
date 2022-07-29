/**
 * @file types.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Some convenience type definitions.
 */
#ifndef TILEDB_DAG_TYPES_HP
#define TILEDB_DAG_TYPES_HP

#include "../fsm.h"
#include "../policies.h"

namespace tiledb::common {

using NullMover3 = ItemMover<NullPolicy, three_stage, size_t>;
using NullMover2 = ItemMover<NullPolicy, two_stage, size_t>;
using DebugMover3 = ItemMover<DebugPolicy, three_stage, size_t>;
using DebugMover2 = ItemMover<DebugPolicy, two_stage, size_t>;
using ManualMover3 = ItemMover<ManualPolicy, three_stage, size_t>;
using ManualMover2 = ItemMover<ManualPolicy, two_stage, size_t>;
using AsyncMover3 = ItemMover<AsyncPolicy, three_stage, size_t>;
using AsyncMover2 = ItemMover<AsyncPolicy, two_stage, size_t>;
using UnifiedAsyncMover3 = ItemMover<UnifiedAsyncPolicy, three_stage, size_t>;
using UnifiedAsyncMover2 = ItemMover<UnifiedAsyncPolicy, two_stage, size_t>;

using NullPolicy3 = NullPolicy<NullMover3, three_stage>;
using NullPolicy2 = NullPolicy<NullMover2, two_stage>;
using DebugPolicy3 = DebugPolicy<DebugMover3, three_stage>;
using DebugPolicy2 = DebugPolicy<DebugMover2, two_stage>;
using ManualPolicy3 = ManualPolicy<ManualMover3, three_stage>;
using ManualPolicy2 = ManualPolicy<ManualMover2, two_stage>;
using AsyncPolicy3 = AsyncPolicy<AsyncMover3, three_stage>;
using AsyncPolicy2 = AsyncPolicy<AsyncMover2, two_stage>;
using UnifiedAsyncPolicy3 = UnifiedAsyncPolicy<UnifiedAsyncMover3, three_stage>;
using UnifiedAsyncPolicy2 = UnifiedAsyncPolicy<UnifiedAsyncMover2, two_stage>;

using NullStateMachine3 = PortFiniteStateMachine<NullPolicy3, three_stage>;
using NullStateMachine2 = PortFiniteStateMachine<NullPolicy2, two_stage>;
using DebugStateMachine3 = PortFiniteStateMachine<DebugPolicy3, three_stage>;
using DebugStateMachine2 = PortFiniteStateMachine<DebugPolicy2, two_stage>;
using ManualStateMachine3 = PortFiniteStateMachine<ManualPolicy3, three_stage>;
using ManualStateMachine2 = PortFiniteStateMachine<ManualPolicy2, two_stage>;
using AsyncStateMachine3 = PortFiniteStateMachine<AsyncPolicy3, three_stage>;
using AsyncStateMachine2 = PortFiniteStateMachine<AsyncPolicy2, two_stage>;
using UnifiedAsyncStateMachine3 =
    PortFiniteStateMachine<UnifiedAsyncPolicy3, three_stage>;
using UnifiedAsyncStateMachine2 =
    PortFiniteStateMachine<UnifiedAsyncPolicy2, two_stage>;

}  // namespace tiledb::common
#endif  // TILEDB_DAG_TYPES_HP
