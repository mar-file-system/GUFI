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



#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"
#include "utils.h"

typedef struct Counters {
    size_t dirs;
    size_t nondirs;
} Counters_t;

struct PoolArgs {
    QPTPool_enqueue_dst_t (*enqueue)(QPTPool_ctx_t *, QPTPool_f, void *);
    Counters_t *counters;
};

int processdir(QPTPool_ctx_t *ctx, void *data) {
    const size_t id = QPTPool_get_id(ctx);
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);

    Counters_t *counters = pa->counters;
    counters[id].dirs++;

    char *path = (char *) data;

    DIR *dir = opendir(path);
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open path \"%s\": %s (%d)\n",
                path, strerror(err), err);
        return 1;
    }

    const size_t path_len = strlen(path);

    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        if (entry->d_type == DT_DIR) {
            const size_t entry_len = strlen(entry->d_name);

            if (((entry_len == 1) && !strncmp(entry->d_name, ".",  1)) ||
                ((entry_len == 2) && !strncmp(entry->d_name, "..", 2))) {
                continue;
            }

            const size_t subdir_len = path_len + 1 + entry_len;
            char *cp = malloc(subdir_len + 1);
            snprintf(cp, subdir_len + 1, "%s/%s", path, entry->d_name);
            pa->enqueue(ctx, processdir, cp);
        }
        else {
            counters[id].nondirs++;
        }
    }

    closedir(dir);
    free(path);

    return 0;
}

static void sub_help(void) {
   printf("<b|d>             breadth/depth first traversal");
   printf("dir...            walk one or more trees\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        FLAG_END
    };

    struct input in;
    process_args_and_maybe_exit(options, 2, "<b|d> dir...", &in);

    const char *pos0 = argv[idx++];
    if (!strlen(pos0)) {
        fprintf(stderr, "Error: Empty tree traversal type\n");
        input_fini(&in);
        return EXIT_FAILURE;
    }

    struct PoolArgs pa = {
        .enqueue = NULL,
        .counters = calloc(in.maxthreads, sizeof(pa.counters[0])),
    };

    const char traversal = pos0[0];
    switch (traversal) {
        case 'b':
            pa.enqueue = QPTPool_enqueue;
            break;
        case 'd':
            pa.enqueue = QPTPool_enqueue_front;
            break;
        default:
            fprintf(stderr, "Error: Bad tree traversal type: %c\n", traversal);
            free(pa.counters);
            input_fini(&in);
            return EXIT_FAILURE;
    }

    QPTPool_ctx_t *pool = QPTPool_init(in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        free(pa.counters);
        input_fini(&in);
        return EXIT_FAILURE;
    }

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    for(int i = idx; i < argc; i++) {
        const size_t len = strlen(argv[i]);
        char *cp = malloc(len + 1);
        SNFORMAT_S(cp, len + 1, 1, argv[i], len);
        pa.enqueue(pool, processdir, cp);
    }

    QPTPool_stop(pool);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    QPTPool_destroy(pool);

    size_t tot_dirs = 0;
    size_t tot_nondirs = 0;
    for(size_t i = 0; i < in.maxthreads; i++) {
        tot_dirs += pa.counters[i].dirs;
        tot_nondirs += pa.counters[i].nondirs;
    }
    free(pa.counters);

    fprintf(stderr, "Total Dirs:         %zu\n",    tot_dirs);
    fprintf(stderr, "Total NonDirs:      %zu\n",    tot_nondirs);
    fprintf(stderr, "Time Spent Walking: %.4Lfs\n", processtime);

    input_fini(&in);

    return EXIT_SUCCESS;
}
