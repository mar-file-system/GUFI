#include "QueuePerThreadPool.hpp"

QPTPool::QPTPool(const std::size_t threads, Func_t func, struct work & first_work_item, void * extra_args)
    : keep_running(0),
      state(threads)
{
    if (!threads) {
        throw std::runtime_error("0 threads specified for thread pool");
    }

    // put the first work item on the queue
    std::lock_guard <std::mutex> lock(state[0].first.mutex);
    state[0].first.queue.emplace_back(std::move(first_work_item));
    keep_running = 1;

    for(std::size_t id = 0; id < threads; id++) {
        state[id].first.id = id;
        state[id].second = std::thread(&QPTPool::worker_function, this, func, id, extra_args);
    }
}

void QPTPool::enqueue(struct work & new_work, std::size_t & next_queue) {
    // put the previous work on the queue
    {
        std::lock_guard <std::mutex> lock(state[next_queue].first.mutex);
        state[next_queue].first.queue.emplace_back(std::move(new_work));
        keep_running++;
        state[next_queue].first.cv.notify_all();
    }

    // round robin
    next_queue++;
    next_queue %= state.size();
}

std::size_t QPTPool::worker_function(Func_t func, const size_t id, void *args) {
    PerThread<struct work> & tw = state[id].first;
    std::size_t next_queue = id;

    std::size_t processed = 0;
    while (true) {
        std::list <struct work> dirs;
        {
            std::unique_lock <std::mutex> lock(tw.mutex);

            // wait for work
            while (keep_running && !tw.queue.size()) {
                tw.cv.wait(lock);
            }

            if (!keep_running && !tw.queue.size()) {
                break;
            }

            // take all work
            dirs = std::move(tw.queue);
            tw.queue.clear();
        }

        // process all work
        for(struct work & dir : dirs) {
            processed += func(this, dir, id, next_queue, args);
        }

        keep_running -= dirs.size();

        for(size_t i = 0; i < state.size(); i++) {
            state[i].first.cv.notify_all();
        }
    }

    return processed;
}

void QPTPool::wait() {
    for(size_t i = 0; i < state.size(); i++) {
        if (state[i].second.joinable()) {
            state[i].second.join();
        }
    }
}

void QPTPool::stop() {
    keep_running = 0;
    for(size_t i = 0; i < state.size(); i++) {
        state[i].first.cv.notify_all();
    }
    wait();
}
