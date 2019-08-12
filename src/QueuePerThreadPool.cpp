/*
This file is part of GUFI, which is part of MarFS, which is released
under the BSD license.


Copyright (c) 2017, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
Copyright 2017. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



#include "QueuePerThreadPool.hpp"

QPTPool::QPTPool(const std::size_t threads)
    : incomplete(0),
      state(threads)
{
    if (!threads) {
        throw std::runtime_error("0 threads specified for thread pool");
    }

    for(std::size_t id = 0; id < threads; id++) {
        state[id].first.id = id;
    }
}

void QPTPool::enqueue(struct work & new_work) {
    static std::size_t next_queue = 0;
    enqueue(new_work, next_queue);
}

void QPTPool::enqueue(struct work & new_work, std::size_t & next_queue) {
    // put the work on the queue
    {
        std::lock_guard <std::mutex> lock(state[next_queue].first.mutex);
        state[next_queue].first.queue.emplace_back(std::move(new_work));
        incomplete++;
        state[next_queue].first.cv.notify_all();
    }

    // round robin
    next_queue++;
    next_queue %= state.size();
}

void QPTPool::start(Func_t func, void * extra_args) {
    for(WorkPair & wp : state) {
        wp.second = std::thread(&QPTPool::worker_function, this, func, wp.first.id, extra_args);
    }
}

void QPTPool::worker_function(Func_t func, const size_t id, void *args) {
    PerThread<struct work> & tw = state[id].first;
    std::size_t next_queue = id;

    while (true) {
        std::list <struct work> dirs;
        {
            std::unique_lock <std::mutex> lock(tw.mutex);

            // wait for work
            while (incomplete && !tw.queue.size()) {
                tw.cv.wait(lock);
            }

            if (!incomplete && !tw.queue.size()) {
                break;
            }

            // take all work
            dirs = std::move(tw.queue);
            tw.queue.clear();
        }

        // process all work
        for(struct work & dir : dirs) {
            tw.threads_successful += func(this, dir, id, next_queue, args);
        }
        tw.threads_started += dirs.size();

        incomplete -= dirs.size();

        for(size_t i = 0; i < state.size(); i++) {
            state[i].first.cv.notify_all();
        }
    }
}

void QPTPool::wait() {
    for(size_t i = 0; i < state.size(); i++) {
        if (state[i].second.joinable()) {
            state[i].second.join();
        }
    }
}

std::size_t QPTPool::get_index(const std::thread::id & id) const {
    for(WorkPair const & wp : state) {
        if (wp.second.get_id() == id) {
            return wp.first.id;
        }
    }
    return state.size();
}

std::size_t QPTPool::threads_started() const {
    std::size_t sum = 0;
    for(WorkPair const & wp : state) {
        sum += wp.first.threads_started;
    }
    return sum;
}

std::size_t QPTPool::threads_completed() const {
    std::size_t sum = 0;
    for(WorkPair const & wp : state) {
        sum += wp.first.threads_successful;
    }
    return sum;
}
