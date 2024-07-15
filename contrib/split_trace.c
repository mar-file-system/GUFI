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



#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "utils.h"

const size_t GETLINE_DEFAULT_SIZE = 256; /* magic number */

struct Range {
    int fd;
    off_t start, end; /* [start, end) */
    size_t id;
};

static int copy_range(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    (void) ctx; (void) id;

    struct Range *range = (struct Range *) data;
    const char *prefix = (char *) args;

    char outname[MAXPATH];
    SNPRINTF(outname, sizeof(outname), "%s.%zu", prefix, range->id);

    const int dst = open(outname, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (dst < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Unable to open file %s to write range [%jd, %jd): %s (%d)\n",
                outname, (intmax_t) range->start, (intmax_t) range->end, strerror(err), errno);
        free(range);
        return 1;
    }

    const off_t size = range->end - range->start;

    /* ignore errors */
    ftruncate(dst, size);

    const int rc = (copyfd(range->fd, range->start, dst, 0, size) != (ssize_t) size);

    close(dst);
    free(range);

    return rc;
}

/*
 * Find end of current chunk/stanza
 * Return:
 *     -1 - error
 *      0 - found end
 *      1 - end of file
 */
int find_end(const int fd, const char delim, off_t *end) {
    /* Not checking arguments */

    char *line = NULL;
    size_t size = 0;
    ssize_t len = 0;

    /*
     * drop first line - assume started in middle of line, and
     * can't be sure if first delimiter found is actually
     * separator for first and second columns
     *
     * errors are ignored
     */
    getline_fd(&line, &size, fd, end, GETLINE_DEFAULT_SIZE);

    while (1) {
        const off_t before_read = *end;

        free(line);
        line = NULL;
        size = 0;

        len = getline_fd(&line, &size, fd, end, GETLINE_DEFAULT_SIZE);
        if (len == 0) {
            free(line);
            return 1;
        }
        else if (len < 0) {
            return -1;
        }

        const char *first_delim = memchr(line, delim, len);

        /* no delimiter */
        if (!first_delim) {
            fprintf(stderr, "Error: Line at offset %jd does not have a delimiter\n", (intmax_t) before_read);
            free(line);
            return -1;
        }

        if (first_delim[1] == 'd') {
            *end = before_read;
            free(line);
            return 0;
        }
    }

    return -1;
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Syntax: %s src_trace delim max_split_count dst_trace_prefix [threads]\n", argv[0]);
        return 1;
    }

    const char *src = argv[1];
    const char delim = argv[2][0];
    size_t count = 0;
    char *prefix = argv[4];
    size_t threads = 1;

    if (sscanf(argv[3], "%zu", &count) != 1) {
        fprintf(stderr, "Error: Invalid output count: %s\n", argv[2]);
        return 1;
    }

    if (argc > 5) {
        if (sscanf(argv[5], "%zu", &threads) != 1) {
            fprintf(stderr, "Error: Invalid thread count: %s\n", argv[5]);
            return 1;
        }
    }

    const int fd = open(src, O_RDONLY);
    if (fd < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open source trace file %s: %s (%d)\n",
                src, strerror(err), err);
        return 1;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        const int err = errno;
        fprintf(stderr, "Could not fstat trace file: %s (%d)\n",
                strerror(err), err);
        close(fd);
        return 1;
    }

    QPTPool_t *pool = QPTPool_init(threads, prefix);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool with %zu threads\n", threads);
        QPTPool_destroy(pool);
        close(fd);
        return 1;
    }

    /* approximate number of bytes per chunk */
    const size_t jump = (st.st_size / count) + !!(st.st_size % count);

    size_t id = 0;
    off_t start = 0;
    off_t end = start + jump;

    int rc = 0;
    while ((start < st.st_size) &&
           ((rc = find_end(fd, delim, &end)) == 0)) {
        struct Range *range = malloc(sizeof(*range));
        range->fd = fd;
        range->start = start;
        range->end = end;
        range->id = id++;

        QPTPool_enqueue(pool, 0, copy_range, range);

        /* jump to next chunk */
        start = end;
        end += jump;
    }

    /* need this because last chunk will error out of loop */
    if ((rc > -1) && (start < st.st_size)) {
        struct Range *range = malloc(sizeof(*range));
        range->fd = fd;
        range->start = start;
        range->end = st.st_size;
        range->id = id++;

        QPTPool_enqueue(pool, 0, copy_range, range);
    }

    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    close(fd);

    /* error or no trace files created */
    return (rc < 0) || (id == 0);
}
