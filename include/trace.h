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



#ifndef GUFI_TRACE_H
#define GUFI_TRACE_H

#include <pthread.h>
#include <stdio.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"

#ifdef __cplusplus
extern "C" {
#endif

/* write a mapping from an external db path to an attach name */
int externaltofile(FILE *file, const char delim, const char *path);

/* write a work struct to a file */
int worktofile(FILE *file, const char delim, const size_t prefix_len, struct work *work, struct entry_data *ed);

/* convert a formatted string to a work struct or attach name */
int linetowork(char *line, const size_t len, const char delim,
               struct work **work, struct entry_data *ed);

int *open_traces(char **trace_names, size_t trace_count);
void close_traces(int *traces, size_t trace_count);

/* Data stored during first pass of input file */
struct row {
    int trace;
    size_t first_delim;
    char *line;
    size_t len;
    off_t offset;
    size_t entries;
};

struct row *row_init(const int trace, const size_t first_delim, char *line,
                     const size_t len, const long offset);
void row_destroy(struct row **ref);

struct TraceRange {
    int fd;
    off_t start, end; /* [start, end) */
};

struct ScoutTraceStats {
    pthread_mutex_t *mutex; /* lock for values below */

    size_t remaining;

    struct start_end time;  /* time from beginning of enqueue_traces call until right before scout_end_print */
    uint64_t thread_time;   /* sum across all threads */
    size_t files;
    size_t dirs;
    size_t empty;           /* number of directories without files/links
                             * can still have child directories */
};

struct ScoutTraceArgs {
    char delim;
    const char *tracename;
    struct TraceRange tr;

    /* data argument will be a struct row */
    QPTPoolFunc_t processdir;

    /* if provided, call at end of scout_trace */
    void (*free)(void *);

    /* reference to single set of shared values */
    struct ScoutTraceStats *stats;
};

int scout_trace(QPTPool_t *ctx, const size_t id, void *data, void *args);

size_t enqueue_traces(char **traceames, int *tracefds, const size_t trace_count,
                      const char delim, const size_t max_parts,
                      QPTPool_t *ctx, QPTPoolFunc_t func,
                      struct ScoutTraceStats *stats);

#ifdef __cplusplus
}
#endif

#endif
