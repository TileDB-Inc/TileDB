//
// Copyright(c) 2015 Gabi Melman.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)
//

#pragma once
//
// base sink templated over a mutex (either dummy or real)
// concrete implementation should only override the _sink_it method.
// all locking is taken care of here so no locking needed by the implementers..
//

#include "spdlog/sinks/sink.h"
#include "spdlog/formatter.h"
#include "spdlog/common.h"
#include "spdlog/details/log_msg.h"

#include <mutex>

namespace spdlog
{
namespace sinks
{
template<class Mutex>
class base_sink:public sink
{
public:
    base_sink():_mutex() {}
    virtual ~base_sink() = default;

    base_sink(const base_sink&) = delete;
    base_sink& operator=(const base_sink&) = delete;

    void log(const details::log_msg& msg) SPDLOG_FINAL override
    {
        std::lock_guard<Mutex> lock(_mutex);
        _sink_it(msg);
    }
    void flush() SPDLOG_FINAL override
    {
        _flush();
    }

protected:
    virtual void _sink_it(const details::log_msg& msg) = 0;
    virtual void _flush() = 0;
    Mutex _mutex;
};
}
}
