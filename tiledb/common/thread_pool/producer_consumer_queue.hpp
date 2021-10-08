

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>

template <typename Item>
class producer_consumer_queue {

public:
  producer_consumer_queue()                                     = default;
  producer_consumer_queue(const producer_consumer_queue<Item>&) = delete;
  producer_consumer_queue& operator=(const producer_consumer_queue<Item>&) = delete;

  auto push(const Item& item) {
    std::unique_lock lock{mutex_};
    if (!is_open_) {
      return false;
    }

    queue_.push(item);
    // lock.unlock();
    cv_.notify_one();
    return true;
  }

  std::optional<Item> try_pop() {
    std::unique_lock lock{ mutex_ };

    if (queue_.empty()) {   // if ((!is_open_ && queue_.empty()) || queue_.empty()) {
      return {};
    }
    Item item = queue_.front();
    queue_.pop();
    return item;
  }

  std::optional<Item> pop() {
    std::unique_lock lock{mutex_};

    cv_.wait(lock, [this]() { return !is_open_ || !queue_.empty(); });
    if (!is_open_ && queue_.empty()) {
      return {};
    }
    Item item = queue_.front();
    queue_.pop();
    return item;
  }

  size_t size() const {
    std::scoped_lock lock{mutex_};
    return queue_.size();
  }

  bool is_open() const {
    std::scoped_lock lock{mutex_};
    return is_open_;
  }

  auto drain() {
    std::scoped_lock lock{mutex_};
    is_open_ = false;
    // lock.unlock();
    cv_.notify_all();
  }

  void signal_one() {
    cv_.notify_one();
  }

  void signal_all() {
    cv_.notify_all();
  }

private:
  bool empty() const { return queue_.empty(); }

private:
  std::queue<Item>        queue_;
  std::condition_variable cv_;
  mutable std::mutex      mutex_;
  std::atomic<bool>       is_open_{true};
};
