

#include "producer_consumer_queue.hpp"

#include <functional>
#include <future>

class thread_pool {

public:
  thread_pool(size_t n = std::thread::hardware_concurrency()) {
    threads_.reserve(n);
    
    for (size_t i = 0; i < n; ++i) {
      std::thread tmp;
      
      size_t tries = 3;
      while (tries--) {
	try {
	  tmp = std::thread(&thread_pool::worker, this);
	}
	catch (const std::system_error& e) {
	  if (e.code() != std::errc::resource_unavailable_try_again) {
	    throw;
	  }
	  continue;
	}
	break;
      }

      threads_.emplace_back(std::move(tmp));
    }
  }

  ~thread_pool() { shutdown(); }

  template <class Fn, class... Args, class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::shared_future<R> async(const Fn& task, const Args&... args) {

    std::shared_ptr<std::promise<R>> task_promise(new std::promise<R>);
    std::shared_future<R>            future = task_promise->get_future();

    task_queue_.push([task, args..., task_promise] {
      try {
        if constexpr (std::is_void_v<R>) {
          task(args...);
          task_promise->set_value();
        } else {
          task_promise->set_value(task(args...));
        }
      } catch (...) {
        try {
          task_promise->set_exception(std::current_exception());
        } catch (...) {
        }
      }
    });

    return future;
  }

  void worker() {
    while (true) {
      auto val = task_queue_.pop();
      if (val) {
        (*val)();
      } else {
        break;
      }
    }
  }

  void shutdown() {
    task_queue_.drain();
    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

private:
  producer_consumer_queue<std::function<void()>> task_queue_;
  std::vector<std::thread>                       threads_;
  std::atomic<size_t>                            concurrency_level_;
};
