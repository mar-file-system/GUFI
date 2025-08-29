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
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "descend.h"
#include "OutputBuffers.h"
#include "print.h"
#include "utils.h"

struct PoolArgs {
    struct input in;
    struct OutputBuffers obufs;
};

void sub_help(void) {
   printf("input_dir...      walk one or more trees to search files\n");
   printf("\n");
}

struct QPTPool_vals {
    QPTPool_t *ctx;
    size_t id;
    struct PoolArgs *pa;
};

static int format_output(const char *format, const char *path, struct PrintArgs *print_args,
                         size_t root_len) {
    char output[4096];
    char *out = output;

    while (*format && (out < output + sizeof(output) - 1)) {
        if (*format == '\\') {
            format++; /* Skip the backslash */
            unsigned char escape = 0;
            switch (*format) {
                case 'n':
                    escape = '\n';
                    break;
                case 't':
                    escape = '\t';
                    break;
                case 'r':
                    escape = '\r';
                    break;
                case '\\':
                    escape = '\\';
                    break;
                default:
                    *out++ = '\\';
                    escape = *format;
                    break;
            }
            *out++ = escape;
        }
        else if (*format == '%') {
            format++; /* Skip % */

            if (!*format) {
                fprintf(stderr, "parallel_find: bad format sequence at end\n");
                break;
            }

            switch (*format) {
                case 'P': /* Starting-point removed from path */
                    out += snprintf(out, sizeof(output) - (out - output), "%s", path + root_len); 
                    break;
                case 'p':
                    out += snprintf(out, sizeof(output) - (out - output), "%s", path);
                    break;
                default:
                    *out++ = '?';
                    break;
            }
        }
        else {
            *out++ = *format;
        }

        format++;
    }
    
    *out = '\0';

    char *buf[] = {output}; 
    print_parallel(print_args, 1, buf, NULL);
    return 0;
}

static int process_output(struct work *work, struct entry_data *ed, void *nondir_args) {
    (void) ed;
    struct QPTPool_vals *args = (struct QPTPool_vals *) nondir_args;
    const size_t user_types = args->pa->in.filter_types;

    const struct stat *st = &work->statuso;
    if (!((S_ISREG(st->st_mode) && (user_types & FILTER_TYPE_FILE)) || 
          (S_ISDIR(st->st_mode) && (user_types & FILTER_TYPE_DIR))  ||
          (S_ISLNK(st->st_mode) && (user_types & FILTER_TYPE_LINK)))) {
        return 0;
    }

    /*
    When this function is called in processdir, descend is not called yet
    If the level is not checked here, the work->name will be printed even if
    the level is not in the range [min_level, max_level]

    Example:
    root_dir/ --- level 0
    ├── dir1/ --- level 1
    │   └── file1.txt --- level 1

    parallel_find -n -y 1 -z 1
    The expected output is:
    root_dir/dir1
    root_dir/dir1/file1.txt

    But if the level is not checked here, the output will be:
    root_dir/       <- first time print_to_screen is called in processdir before descend is called
    root_dir/dir1
    root_dir/dir1/file1.txt
    */

    if ((work->level < args->pa->in.min_level) || (work->level > args->pa->in.max_level)) {
       return 0;
    }

    struct PrintArgs print = {
        .output_buffer = &args->pa->obufs.buffers[args->id],
        .delim = '\0', 
        .mutex = args->pa->obufs.mutex,
        .outfile = stdout,
        .types = NULL,
        .suppress_newline = args->pa->in.format_set,
    };

    if (args->pa->in.format_set) {
        format_output(args->pa->in.format.data, work->name, &print, work->orig_root.len);
    } else {
        print_parallel(&print, 1, &(work->name), NULL);
    }

    return 0;
}

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct work *work = (struct work *) data;
    struct PoolArgs *pa = (struct PoolArgs *) args;

    int rc = 0;
    
    struct QPTPool_vals qptp_vals = {
        .ctx = ctx,
        .id = id,
        .pa = pa,
    };

    process_output(work, NULL, &qptp_vals);
  
    DIR *dir = opendir(work->name);
    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\"\n", work->name);
        rc = 1;
    }
    else {
        descend(ctx, id, args, &pa->in,
                work, dir, 0, processdir,
                process_output,
                &qptp_vals, NULL);
    }

    closedir(dir);
    free(work);
    return rc;
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;
    process_args_and_maybe_exit("hHvn:f:y:z:t:", 1, "input_dir...", &pa.in);
    int rc = 0;

    /* Initialize output buffers */
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    if (!OutputBuffers_init(&pa.obufs, pa.in.maxthreads, pa.in.output_buffer_size, &mutex)) {
        fprintf(stderr, "Error: Could not initialize %zu output buffers\n", pa.in.maxthreads);
        rc = 1;
        goto cleanup_exit;
    }

    /* create a queue per thread pool */
    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        rc = 1;
        goto cleanup_qptp;
    }
  
    for (int i = idx; i < argc; i++) {
        const char *path = argv[i];
        const size_t path_len = strlen(path);

        struct work *work = new_work_with_name(NULL, 0, path, path_len);
        work->orig_root.data = path;
        /* add 1 to remove the slash added from descend */
        work->orig_root.len = path_len + 1;
        if (!work) {
            fprintf(stderr, "Error: Failed to create work item for \"%s\"\n", path);
            rc = 1;
            continue;
        }

        /* input path that are links are followed */ 
        if (stat(path, &work->statuso) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Cannot stat \"%s\": %s (%d)\n",
                    path, strerror(err), err);
            free(work);
            rc = 1;
            continue;
        }

        if (S_ISREG(work->statuso.st_mode)) {
            struct QPTPool_vals qptp_vals = {
                .ctx = pool,
                .id = 0,
                .pa = &pa,
            };
            process_output(work, NULL, &qptp_vals);
        }
        else if (S_ISDIR(work->statuso.st_mode)) {
            QPTPool_enqueue(pool, 0, processdir, work);
        }
        else {
            fprintf(stderr, "Error: Unsupported type for \"%s\"\n", work->name);
            free(work);
            rc = 1;
        }
    }

    QPTPool_stop(pool);

    const uint64_t threads_started   = QPTPool_threads_started(pool);
    const uint64_t threads_completed = QPTPool_threads_completed(pool);

  cleanup_qptp:
    QPTPool_destroy(pool);

    OutputBuffers_flush_to_single(&pa.obufs, stdout);
    OutputBuffers_destroy(&pa.obufs);

  cleanup_exit:
    input_fini(&pa.in);

    return ((rc == 0) && (threads_started == threads_completed)) ? EXIT_SUCCESS : EXIT_FAILURE;
}
