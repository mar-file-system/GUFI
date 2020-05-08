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



#include "debug.h"

pthread_mutex_t print_mutex = PTHREAD_MUTEX_INITIALIZER;

uint64_t epoch = 0;

uint64_t since_epoch(struct timespec * ts) {
    uint64_t ns = ts->tv_sec;
    ns *= 1000000000ULL;
    ns += ts->tv_nsec;
    return ns;
}

long double elapsed(struct start_end * se) {
    const long double s = ((long double) se->start.tv_sec) + ((long double) se->start.tv_nsec) / 1000000000ULL;
    const long double e = ((long double) se->end.tv_sec)   + ((long double) se->end.tv_nsec)   / 1000000000ULL;
    return e - s;
}

int print_timer(struct OutputBuffers * obufs, const size_t id, char * str, const size_t size, const char * name, struct start_end * se) {
    const size_t len = snprintf(str, size, "%zu %s %" PRIu64 " %" PRIu64 "\n", id, name, since_epoch(&se->start) - epoch, since_epoch(&se->end) - epoch);
    const size_t capacity = obufs->buffers[id].capacity;

    /* if the row can fit within an empty buffer, try to add the new row to the buffer */
    if (len <= capacity) {
        /* if there's not enough space in the buffer to fit the new row, flush it first */
        if ((obufs->buffers[id].filled + len) > capacity) {
            OutputBuffer_flush(&obufs->mutex, &obufs->buffers[id], stderr);
        }

        char * buf = obufs->buffers[id].buf;
        size_t filled = obufs->buffers[id].filled;

        memcpy(&buf[filled], str, len);
        filled += len;

        obufs->buffers[id].filled = filled;
        obufs->buffers[id].count++;
    }
    else {
        /* if the row does not fit the buffer, output immediately */
        /* instead of buffering since order shouldn't matter      */
        fwrite(str, sizeof(char), len, stderr);
    }

    return 0;
}
