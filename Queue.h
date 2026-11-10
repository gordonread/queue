#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <utility>

template <typename T>
class Queue final
{
public:
    Queue() = default;
    ~Queue() = default;

    Queue(const Queue&) = delete;
    Queue(Queue&&) = delete;
    Queue& operator=(const Queue&) = delete;
    Queue& operator=(Queue&&) = delete;

    bool empty() const
    {
        std::lock_guard<std::mutex> guard{mutex_};
        return queue_.empty();
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> guard{mutex_};
        return queue_.size();
    }

    bool push(const T& value)
    {
        return enqueueAndNotify([&]() { queue_.push(value); });
    }

    bool push(T&& value)
    {
        return enqueueAndNotify([&]() { queue_.push(std::move(value)); });
    }

    template <typename... Args>
    bool emplace(Args&&... args)
    {
        return enqueueAndNotify([&]() { queue_.emplace(std::forward<Args>(args)...); });
    }

    bool pop(T& value)
    {
        if (!isRunning_.load(std::memory_order_acquire))
        {
            return false;
        }
        std::unique_lock<std::mutex> guard{mutex_};
        cv_.wait(guard, [&]() { return !queue_.empty() || !isRunning_.load(std::memory_order_relaxed); });
        if (!isRunning_.load(std::memory_order_relaxed))
        {
            return false;
        }

        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void swap(Queue& rhs)
    {
        if (this == &rhs)
        {
            return;
        }

        std::scoped_lock guards{mutex_, rhs.mutex_};
        queue_.swap(rhs.queue_);

        cv_.notify_all();
        rhs.cv_.notify_all();
    }

    void stop()
    {
        isRunning_.store(false, std::memory_order_release);

        std::queue<T> drain{};
        {
            std::lock_guard<std::mutex> guard{mutex_};
            drain.swap(queue_);
        }
        cv_.notify_all();
    }

    bool running() const noexcept { return isRunning_.load(std::memory_order_acquire); }

private:

    template <typename EnqueueFn>
    bool enqueueAndNotify(EnqueueFn&& enqueueFn)
    {
        if (!isRunning_.load(std::memory_order_acquire))
        {
            return false;
        }
        {
            std::lock_guard<std::mutex> guard{mutex_};
            if (!isRunning_.load(std::memory_order_relaxed))
            {
                return false;
            }
            enqueueFn();
        }
        cv_.notify_one();
        return true;
    }

    mutable std::mutex mutex_{};
    std::condition_variable cv_{};
    std::queue<T> queue_{};
    std::atomic<bool> isRunning_{true};
};
