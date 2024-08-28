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
#include "trace.h"
#include "utils.h"
#include "xattrs.h"

struct PoolArgs {
    struct input in;
    size_t src_dirname_len;
};

/* TODO: possible optimization - pass in and modify parent name by adding entry's name to save on some copying */
static int process_entries(struct input *in,
                           struct work *entry, struct entry_data *ed) {
    char path[MAXPATH];
    SNFORMAT_S(path, sizeof(path), 3,
               in->nameto.data, in->nameto.len,
               "/", (size_t) 1,
               entry->name, entry->name_len);

    switch (ed->type) {
        case 'f':
            ;
            int fd = open(path, O_WRONLY | O_TRUNC | O_CREAT, ed->statuso.st_mode);
            if (fd < 0) {
                const int err = errno;
                fprintf(stderr, "Error opening file %s: %d\n", path, err);
            }
            close(fd);

            set_metadata(path, &ed->statuso, &ed->xattrs);

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
static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    /* Not checking arguments */

    (void) ctx; (void) id;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;
    struct row *w = (struct row *) data;
    const int trace = w->trace;

    struct work dir; /* name and name_len are not used */
    struct entry_data ed;

    /* parse the directory data */
    linetowork(w->line, w->len, in->delim, &dir, &ed);

    /* create the directory */
    char topath[MAXPATH];
    SNFORMAT_S(topath, MAXPATH, 3,
               in->nameto.data, in->nameto.len,
               "/", (size_t) 1,
               w->line, w->first_delim);

    /* have to dupdir here because directories can show up in any order */
    if (dupdir(topath, &ed.statuso)) {
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
        const ssize_t len = getline_fd(&line, &size, trace, &w->offset, GETLINE_DEFAULT_SIZE);
        if (len < 1) {
            break;
        }

        struct work row;
        struct entry_data row_ed;

        linetowork(line, len, in->delim, &row, &row_ed);

        if (row_ed.type == 'e') {
            xattrs_cleanup(&row_ed.xattrs);
            continue;
        }

        process_entries(in, &row, &row_ed);
        xattrs_cleanup(&row_ed.xattrs);
    }

    free(line); /* reuse line and only alloc+free once */

    set_metadata(topath, &ed.statuso, &ed.xattrs);

    xattrs_cleanup(&ed.xattrs);
    row_destroy(&w);
    return 0;
}

void sub_help(void) {
   printf("input_dir         walk this GUFI index to produce a tree\n");
   printf("output_dir        reconstruct the tree under here\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHn:d:x", 2, "trace_file... output_dir", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }
    else {
        // parse positional args, following the options
        INSTALL_STR(&pa.in.nameto, argv[argc - 1]);
    }

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

    struct stat st;
    st.st_mode = S_IRWXU | S_IRWXG | S_IRWXO;
    st.st_uid = geteuid();
    st.st_gid = getegid();

    if (dupdir(pa.in.nameto.data, &st)) {
        fprintf(stderr, "Could not create directory %s\n", pa.in.nameto.data);
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    struct QPTPool *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        rc = EXIT_FAILURE;
        goto free_traces;
    }

    /* parse the trace files and enqueue work */
    size_t remaining = 0;
    uint64_t scout_time = 0;
    size_t files = 0;
    size_t dirs = 0;
    size_t empty = 0;
    enqueue_traces(&argv[idx], traces, trace_count, pa.in.delim,
                   pa.in.maxthreads, pool, processdir,
                   &remaining, &scout_time, &files, &dirs, &empty);

    QPTPool_stop(pool);
    QPTPool_destroy(pool);

  free_traces:
    close_traces(traces, trace_count);

    input_fini(&pa.in);
    return rc;
}
