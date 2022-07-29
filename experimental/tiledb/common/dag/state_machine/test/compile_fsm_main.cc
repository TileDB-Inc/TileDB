/**
 * @file compile_fsm_main.cc
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
 */

#include "../fsm.h"
#include "../policies.h"
#include "./types.h"

int main() {
  (void)sizeof(NullMover3);
  (void)sizeof(NullMover2);
  (void)sizeof(NullPolicy3);
  (void)sizeof(NullPolicy2);
  (void)sizeof(NullStateMachine3);
  (void)sizeof(NullStateMachine2);
  (void)sizeof(DebugMover3);
  (void)sizeof(DebugMover2);
  (void)sizeof(DebugPolicy3);
  (void)sizeof(DebugPolicy2);
  (void)sizeof(DebugStateMachine3);
  (void)sizeof(DebugStateMachine2);
  (void)sizeof(ManualMover3);
  (void)sizeof(ManualMover2);
  (void)sizeof(ManualPolicy3);
  (void)sizeof(ManualPolicy2);
  (void)sizeof(ManualStateMachine3);
  (void)sizeof(ManualStateMachine2);
  (void)sizeof(AsyncMover3);
  (void)sizeof(AsyncMover2);
  (void)sizeof(AsyncPolicy3);
  (void)sizeof(AsyncPolicy2);
  (void)sizeof(AsyncStateMachine3);
  (void)sizeof(AsyncStateMachine2);
  (void)sizeof(UnifiedAsyncMover3);
  (void)sizeof(UnifiedAsyncMover2);
  (void)sizeof(UnifiedAsyncPolicy3);
  (void)sizeof(UnifiedAsyncPolicy2);
  (void)sizeof(UnifiedAsyncStateMachine3);
  (void)sizeof(UnifiedAsyncStateMachine2);
}
