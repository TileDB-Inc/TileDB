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

#include "../fsm_types.h"
#include "../item_mover.h"
#include "../policies.h"

namespace tiledb::common {

template <class T>
using NullMover3 = ItemMover<NullPolicy, three_stage, T>;
template <class T>
using NullMover2 = ItemMover<NullPolicy, two_stage, T>;
template <class T>
using DebugMover3 = ItemMover<DebugPolicy, three_stage, T>;
template <class T>
using DebugMover2 = ItemMover<DebugPolicy, two_stage, T>;
template <class T>
using ManualMover3 = ItemMover<ManualPolicy, three_stage, T>;
template <class T>
using ManualMover2 = ItemMover<ManualPolicy, two_stage, T>;
template <class T>
using AsyncMover3 = ItemMover<AsyncPolicy, three_stage, T>;
template <class T>
using AsyncMover2 = ItemMover<AsyncPolicy, two_stage, T>;
template <class T>
using UnifiedAsyncMover3 = ItemMover<UnifiedAsyncPolicy, three_stage, T>;
template <class T>
using UnifiedAsyncMover2 = ItemMover<UnifiedAsyncPolicy, two_stage, T>;

template <class T>
using NullPolicy3 = NullPolicy<NullMover3<T>, three_stage>;
template <class T>
using NullPolicy2 = NullPolicy<NullMover2<T>, two_stage>;
template <class T>
using DebugPolicy3 = DebugPolicy<DebugMover3<T>, three_stage>;
template <class T>
using DebugPolicy2 = DebugPolicy<DebugMover2<T>, two_stage>;
template <class T>
using ManualPolicy3 = ManualPolicy<ManualMover3<T>, three_stage>;
template <class T>
using ManualPolicy2 = ManualPolicy<ManualMover2<T>, two_stage>;
template <class T>
using AsyncPolicy3 = AsyncPolicy<AsyncMover3<T>, three_stage>;
template <class T>
using AsyncPolicy2 = AsyncPolicy<AsyncMover2<T>, two_stage>;
template <class T>
using UnifiedAsyncPolicy3 =
    UnifiedAsyncPolicy<UnifiedAsyncMover3<T>, three_stage>;
template <class T>
using UnifiedAsyncPolicy2 =
    UnifiedAsyncPolicy<UnifiedAsyncMover2<T>, two_stage>;

template <class T>
using NullStateMachine3 = PortFiniteStateMachine<NullPolicy3<T>, three_stage>;
template <class T>
using NullStateMachine2 = PortFiniteStateMachine<NullPolicy2<T>, two_stage>;
template <class T>
using DebugStateMachine3 = PortFiniteStateMachine<DebugPolicy3<T>, three_stage>;
template <class T>
using DebugStateMachine2 = PortFiniteStateMachine<DebugPolicy2<T>, two_stage>;
template <class T>
using ManualStateMachine3 =
    PortFiniteStateMachine<ManualPolicy3<T>, three_stage>;
template <class T>
using ManualStateMachine2 = PortFiniteStateMachine<ManualPolicy2<T>, two_stage>;
template <class T>
using AsyncStateMachine3 = PortFiniteStateMachine<AsyncPolicy3<T>, three_stage>;
template <class T>
using AsyncStateMachine2 = PortFiniteStateMachine<AsyncPolicy2<T>, two_stage>;
template <class T>
using UnifiedAsyncStateMachine3 =
    PortFiniteStateMachine<UnifiedAsyncPolicy3<T>, three_stage>;
template <class T>
using UnifiedAsyncStateMachine2 =
    PortFiniteStateMachine<UnifiedAsyncPolicy2<T>, two_stage>;

}  // namespace tiledb::common
#endif  // TILEDB_DAG_TYPES_HP
