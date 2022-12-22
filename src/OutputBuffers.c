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



#include <stdlib.h>
#include <string.h>

#include "OutputBuffers.h"

struct OutputBuffer *OutputBuffer_init(struct OutputBuffer *obuf, const size_t capacity) {
    if (obuf) {
        if (!(obuf->buf = malloc(capacity))) {
            return NULL;
        }
        obuf->capacity = capacity;
        obuf->filled = 0;
        obuf->count = 0;
    }

    return obuf;
}

size_t OutputBuffer_write(struct OutputBuffer *obuf, const void *buf, const size_t size, const int increment_count) {
    if ((obuf->filled + size) > obuf->capacity) {
        return 0;
    }

    memcpy(obuf->buf + obuf->filled, buf, size);
    obuf->filled += size;
    obuf->count += !!increment_count;

    return size;
}

size_t OutputBuffer_flush(struct OutputBuffer *obuf, FILE *out) {
    /* Not checking arguments */

    const size_t rc = fwrite(obuf->buf, sizeof(char), obuf->filled, out);
    if (rc == obuf->filled) {
        obuf->filled = 0;
    }

    return rc;
}

void OutputBuffer_destroy(struct OutputBuffer *obuf) {
    if (obuf) {
        free(obuf->buf);
        obuf->buf = NULL;
    }
}

struct OutputBuffers *OutputBuffers_init(struct OutputBuffers *obufs, const size_t count, const size_t capacity, pthread_mutex_t *global_mutex) {
    if (!obufs) {
        return NULL;
    }

    obufs->mutex = global_mutex;
    obufs->count = 0;
    if (!(obufs->buffers = malloc(count * sizeof(struct OutputBuffer)))) {
        return NULL;
    }

    for(size_t i = 0; i < count; i++) {
        if (!OutputBuffer_init(&obufs->buffers[i], capacity)) {
            OutputBuffers_destroy(obufs);
            return NULL;
        }
        obufs->count++;
    }

    return obufs;
}

size_t OutputBuffers_flush_to_single(struct OutputBuffers *obufs, FILE *out) {
    /* Not checking arguments */

    if (obufs->mutex) {
        pthread_mutex_lock(obufs->mutex);
    }

    size_t octets = 0;
    for(size_t i = 0; i < obufs->count; i++) {
        octets += OutputBuffer_flush(&obufs->buffers[i], out);
    }

    if (obufs->mutex) {
        pthread_mutex_unlock(obufs->mutex);
    }

    return octets;
}

size_t OutputBuffers_flush_to_multiple(struct OutputBuffers *obufs, FILE **out) {
    /* Not checking arguments */

    if (obufs->mutex) {
        pthread_mutex_lock(obufs->mutex);
    }

    size_t octets = 0;
    for(size_t i = 0; i < obufs->count; i++) {
        const size_t written = OutputBuffer_flush(&obufs->buffers[i], out[i]);
        if ((written < obufs->buffers[i].filled) &&
            obufs->buffers[i].filled) {
            continue;
        }
        octets += written;
    }

    if (obufs->mutex) {
        pthread_mutex_unlock(obufs->mutex);
    }

    return octets;
}

void OutputBuffers_destroy(struct OutputBuffers *obufs) {
    if (obufs) {
        if (obufs->buffers) {
            for(size_t i = 0; i < obufs->count; i++) {
                OutputBuffer_destroy(&obufs->buffers[i]);
            }

            free(obufs->buffers);
            obufs->buffers = NULL;
        }

        obufs->count = 0;
    }
}
