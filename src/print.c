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

#include "print.h"

int print_parallel(void *args, int count, char **data, char **columns) {
    (void) columns;

    PrintArgs_t *print = (PrintArgs_t *) args;
    struct OutputBuffer *ob = print->output_buffer;

    size_t *lens = malloc(count * sizeof(size_t));
    size_t row_len = count + 1; /* one delimiter per column + newline */
    for(int i = 0; i < count; i++) {
        lens[i] = 0;
        if (data[i]) {
            lens[i] = strlen(data[i]);
            row_len += lens[i];
        }
    }

    /* if a row cannot fit the buffer for whatever reason, flush the existing buffer */
    if ((ob->capacity - ob->filled) < row_len) {
        if (print->mutex) {
            pthread_mutex_lock(print->mutex);
        }
        OutputBuffer_flush(ob, print->outfile);
        if (print->mutex) {
            pthread_mutex_unlock(print->mutex);
        }
    }

    /* if the row is larger than the entire buffer, flush this row */
    if (ob->capacity < row_len) {
        /* the existing buffer will have been flushed a few lines ago, maintaining output order */
        if (print->mutex) {
            pthread_mutex_lock(print->mutex);
        }
        for(int i = 0; i < count; i++) {
            if (data[i]) {
                fwrite(data[i], sizeof(char), lens[i], print->outfile);
            }
            fwrite(&print->delim, sizeof(char), 1, print->outfile);
        }
        fwrite("\n", sizeof(char), 1, print->outfile);
        ob->count++;
        if (print->mutex) {
            pthread_mutex_unlock(print->mutex);
        }
    }
    /* otherwise, the row can fit into the buffer, so buffer it */
    /* if the old data + this row cannot fit the buffer, works since old data has been flushed */
    /* if the old data + this row fit the buffer, old data was not flushed, but no issue */
    else {
        char *buf = ob->buf;
        size_t filled = ob->filled;
        for(int i = 0; i < count; i++) {
            if (data[i]) {
                memcpy(&buf[filled], data[i], lens[i]);
                filled += lens[i];
            }

            buf[filled] = print->delim;
            filled++;
        }

        buf[filled] = '\n';
        filled++;

        ob->filled = filled;
        ob->count++;
    }

    free(lens);

    print->rows++;

    return 0;
}
