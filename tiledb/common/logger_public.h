/**
 * @file   logger_public.h
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
 * This file defines simple logging functions that can be exposed (by expedient)
 * to the public API. Their implementations are in `logger.cc`. They are
 * declared in a separate header from `logger.h` for historical reasons, when
 * `logger.h` was leaking spdlog's headers.
 */

#pragma once
#ifndef TILEDB_LOGGER_PUBLIC_H
#define TILEDB_LOGGER_PUBLIC_H

#include "tiledb/common/exception/exception.h"

namespace tiledb {
namespace common {

class Logger;

/** Logs a trace. */
void LOG_TRACE(const std::string& msg);

/** Logs debug. */
void LOG_DEBUG(const std::string& msg);

/** Logs info. */
void LOG_INFO(const std::string& msg);

/** Logs a warning. */
void LOG_WARN(const std::string& msg);

/** Logs an error. */
void LOG_ERROR(const std::string& msg);

/** Logs a status. */
Status LOG_STATUS(const Status& st);

/** Logs a status without returning it. */
void LOG_STATUS_NO_RETURN_VALUE(const Status& st);

/** Logs a status exception. */
void LOG_STATUS(const StatusException& st);

/** Logs trace. */
void LOG_TRACE(const std::stringstream& msg);

/** Logs debug. */
void LOG_DEBUG(const std::stringstream& msg);

/** Logs info. */
void LOG_INFO(const std::stringstream& msg);

/** Logs a warning. */
void LOG_WARN(const std::stringstream& msg);

/** Logs an error. */
void LOG_ERROR(const std::stringstream& msg);

}  // namespace common

/*
 * Make the simple log functions available to the `tiledb` namespace
 */
using common::LOG_DEBUG;
using common::LOG_ERROR;
using common::LOG_INFO;
using common::LOG_STATUS;
using common::LOG_TRACE;
using common::LOG_WARN;

}  // namespace tiledb

#endif  // TILEDB_LOGGER_PUBLIC_H
