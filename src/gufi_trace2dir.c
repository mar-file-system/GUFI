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
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "dbutils.h"
#include "str.h"
#include "trace.h"
#include "utils.h"
#include "xattrs.h"

struct PoolArgs {
    struct input in;
    refstr_t tree_parent; /* actual tree is placed at <tree parent>/<path in trace> */
};

/* TODO: possible optimization - pass in and modify parent name by adding entry's name to save on some copying */
static int process_entries(refstr_t *tree_parent,
                           struct work *entry, struct entry_data *ed) {
    char path[MAXPATH];
    SNFORMAT_S(path, sizeof(path), 3,
               tree_parent->data, tree_parent->len,
               "/", (size_t) 1,
               entry->name, entry->name_len);

    switch (ed->type) {
        case 'f':
            ;
            int fd = open(path, O_WRONLY | O_TRUNC | O_CREAT, entry->statuso.st_mode);
            if (fd < 0) {
                const int err = errno;
                fprintf(stderr, "Error opening file %s: %d\n", path, err);
            }
            close(fd);

            set_metadata(path, &entry->statuso, &ed->xattrs);

            break;
        case 'l':
            ;
            if (symlink(ed->linkname, path) < 0) {
                const int err = errno;
                if (err != EEXIST) {
                    fprintf(stderr, "Error creating symlink %s: %s (%d)\n",
                            path, strerror(err), err);
                }
            }
            break;
        /* case 'd': */
        /* default: */
        /*     fprintf(stderr, "Bad entry type: %c\n", ed->type); */
        /*     return 1; */
    }

    return 0;
}

/* process the work under one directory (no recursion) */
static int processdir(QPTPool_ctx_t *ctx, void *data) {
    /* Not checking arguments */

    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct input *in = &pa->in;
    struct row *w = (struct row *) data;
    const int trace = w->trace;

    // TODO: see if there is a better way to do this
    struct work *dir; /* name and name_len are not used */
    struct entry_data ed;

    /* parse the directory data */
    linetowork(w->line, w->len, in->delim, &dir, &ed);

    /* create the directory */
    char topath[MAXPATH];
    SNFORMAT_S(topath, MAXPATH, 3,
               pa->tree_parent.data, pa->tree_parent.len,
               "/", (size_t) 1,
               w->line, w->first_delim);

    /* have to dupdir here because directories can show up in any order */
    if (dupdir(topath, dir->statuso.st_mode, dir->statuso.st_uid, dir->statuso.st_gid)) {
        const int err = errno;
        fprintf(stderr, "Dupdir failure: \"%s\": %s (%d)\n",
                topath, strerror(err), err);
        xattrs_cleanup(&ed.xattrs);
        row_destroy(&w);
        return 1;
    }

    /* create entries */
    char *line = NULL;
    size_t size = 0;
    for(size_t i = 0; i < w->entries; i++) {
        const ssize_t len = getline_fd_seekable(&line, &size, trace, &w->offset, GETLINE_DEFAULT_SIZE);
        if (len < 1) {
            break;
        }

        struct work *row;
        struct entry_data row_ed;

        // TODO: can linetowork be changed to use realloc to avoid malloc() + free() looping?
        linetowork(line, len, in->delim, &row, &row_ed);

        if (row_ed.type == 'e') {
            xattrs_cleanup(&row_ed.xattrs);
            continue;
        }

        process_entries(&pa->tree_parent, row, &row_ed);
        xattrs_cleanup(&row_ed.xattrs);
        free(row);
    }

    set_metadata(topath, &dir->statuso, &ed.xattrs);

    free(line); /* reuse line and only alloc+free once */
    free(dir);

    xattrs_cleanup(&ed.xattrs);
    row_destroy(&w);
    return 0;
}

void sub_help(void) {
   printf("trace...          read these trace files\n");
   printf("dir               reconstruct the tree under here\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* tree walk flags */
        FLAG_SET_XATTRS,

        /* input flags */
        FLAG_DELIM,

        FLAG_END
    };

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 2, "trace... output_dir", &pa.in);

    // parse positional args, following the options
    INSTALL_STR(&pa.tree_parent, argv[argc - 1]);

    /* open trace files for threads to jump around in */
    /* open the trace files here to not repeatedly open in threads */
    const size_t trace_count = argc - idx - 1;
    int *traces = open_traces(&argv[idx], trace_count);
    if (!traces) {
        fprintf(stderr, "Failed to open trace file for each thread\n");
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    int rc = EXIT_SUCCESS;

    if (dupdir(pa.tree_parent.data, S_IRWXU | S_IRWXG | S_IRWXO, geteuid(), getegid())) {
        fprintf(stderr, "Could not create directory %s\n", pa.tree_parent.data);
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    struct QPTPool_ctx *ctx = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(ctx);
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    /* parse the trace files and enqueue work */
    struct TraceStats stats;
    enqueue_traces(&argv[idx], traces, trace_count,
                   pa.in.delim,
                   /* allow for some threads to start processing while reading */
                   (pa.in.maxthreads / 2) + !!(pa.in.maxthreads & 1),
                   ctx, processdir,
                   &stats);

    QPTPool_stop(ctx);
    QPTPool_destroy(ctx);

  free_traces:
    close_traces(traces, trace_count);

    input_fini(&pa.in);
    return rc;
}
