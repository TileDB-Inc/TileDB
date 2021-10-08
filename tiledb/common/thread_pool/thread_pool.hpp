

#include "producer_consumer_queue.hpp"
// #include "bounded_buffer_queue.hpp"

#include <functional>
#include <future>
#include <mutex>
#include <iostream>

class thread_pool {

private:

public:
  thread_pool(size_t n = std::thread::hardware_concurrency()) : task_queue_() {
    threads_.reserve(n);

    for (size_t i = 0; i < n; ++i) {
      std::thread tmp;

      size_t tries = 3;
      while (tries--) {
        try {
          tmp = std::thread(&thread_pool::worker, this);
        } catch (const std::system_error& e) {
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

#if 0

    auto lambda = [task, args..., task_promise] {
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
    };
      
#if 0
    if (!task_queue_.push(lambda)) {
      lambda();
    }
#endif
#endif

    return future;
  }


  void worker() {
    while (true) {
      if (auto val = task_queue_.pop()) {
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

  template <class Task>
  auto wait_all(std::vector<Task>& tasks) {
    wait_all_status(tasks);
    return true;
  }

  template <class Task>
  auto wait_all_status(std::vector<Task>& tasks) {

    std::queue<Task> pending_tasks;

    // enqueue all tasks that have not completed
    for (auto& task : tasks) {
      if (!task.valid()) {
	std::cout << "invalid task" << std::endl;
	return false;
      }
      auto status = task.wait_for(std::chrono::milliseconds(0));
      if (status != std::future_status::ready) {
	pending_tasks.push(task);
      }
    }
    

    while (!pending_tasks.empty()) {

      auto task = pending_tasks.front(); pending_tasks.pop();
      
      if (pending_tasks.empty()) {
	task.wait();
      } else {
	std::future_status status = task.wait_for(std::chrono::milliseconds(0));
	
	if (status != std::future_status::ready) {
	  if (auto val = task_queue_.try_pop()) {
	    (*val)();
	  } else {
	    std::future_status status = task.wait_for(std::chrono::milliseconds(10));
	  }
	  pending_tasks.push(task);
	}
      }
    }
    
    return true;
  }

private:

  producer_consumer_queue<std::function<void()>> task_queue_;
  std::vector<std::thread>                       threads_;
  std::atomic<size_t>                            concurrency_level_;

  std::mutex mutex_;

  std::mutex io_mutex_;

};
