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



#ifndef QUEUE_PER_THREAD_POOL_H
#define QUEUE_PER_THREAD_POOL_H

#include <pthread.h>

#if defined(DEBUG) && defined(PER_THREAD_STATS)
#include "OutputBuffers.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* The Queue Per Thread Pool context */
struct QPTPoolData;
struct QPTPool {
    struct QPTPoolData * data;
    size_t size;

    pthread_mutex_t mutex;
    int running;
    size_t incomplete;

    #if defined(DEBUG) && defined(PER_THREAD_STATS)
    struct OutputBuffers * buffers;
    #endif

};

/* User defined function to pass into QPTPool_start
 *
 * @param ctx      the pool context the function is running in
 * @param id       the id of this thread
 * @param data     the data the function is operating on
 * @param args     any extra data to make accessible to all functions that this thread pool runs
 * @return 0 if successful, non-zero if not
 */
typedef int (*QPTPoolFunc_t)(struct QPTPool * ctx, const size_t id, void * data, void * args);

/* main functions for operating a QPTPool */

/* initialize a QPTPool context without starting the threads */
struct QPTPool * QPTPool_init(const size_t threads
                              #if defined(DEBUG) && defined(PER_THREAD_STATS)
                              , struct OutputBuffers * buffers
                              #endif
    );

/* start the threads */
size_t QPTPool_start(struct QPTPool * ctx, void * args);

/* enqueue data and a function to process the data */
/* id will push to the thread's next scheduled queue, rather than directly onto queue[id]*/
void QPTPool_enqueue(struct QPTPool * ctx, const size_t id, QPTPoolFunc_t func, void * new_work);

/* wait for all work to be processed and join threads*/
void QPTPool_wait(struct QPTPool * ctx);

/* clean up QPTPool context data */
void QPTPool_destroy(struct QPTPool * ctx);

/* utility functions */

/* get a number in the range [0, # of threads), or (size_t) -1 on error */
size_t QPTPool_get_index(struct QPTPool * ctx, const pthread_t id);

/* get the number of threads that were started by the QPTPool */
size_t QPTPool_threads_started(struct QPTPool * ctx);

/* get the number of started threads that completed successfully */
size_t QPTPool_threads_completed(struct QPTPool * ctx);

#ifdef __cplusplus
}
#endif

#endif
