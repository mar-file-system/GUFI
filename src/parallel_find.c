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
#include <fcntl.h>
#include <grp.h>
#include <pthread.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "OutputBuffers.h"
#include "QueuePerThreadPool.h"
#include "bf.h"
#include "descend.h"
#include "outfiles.h"
#include "print.h"
#include "utils.h"

/* default format used by GNU find */
static const char DEFAULT_FORMAT[] = "%p\n";

struct PoolArgs {
    struct input in;
    struct OutputBuffers obufs;
    FILE **outfiles;
};

struct enqueue_nondir_args {
    QPTPool_ctx_t *ctx;
    size_t id;
    struct PoolArgs *pa;
};

static int format_output(struct work *work, struct entry_data *ed,
                         const char *format, struct PrintArgs *print_args) {
    char output[4096];
    char *out = output;
    int c = 0; /* \c */

    while (*format && (out < output + sizeof(output) - 1) && !c) {
        if (*format == '\\') {
            format++; /* Skip the backslash */
            unsigned char escape = 0;
            switch (*format) {
                case 'a':
                    escape = '\a';
                    break;
                case 'b':
                    escape = '\b';
                    break;
                case 'c':
                    c = 1;
                    break;
                case 'f':
                    escape = '\f';
                    break;
                case 'n':
                    escape = '\n';
                    break;
                case 'r':
                    escape = '\r';
                    break;
                case 't':
                    escape = '\t';
                    break;
                case 'v':
                    escape = '\v';
                    break;
                case '0':
                    escape = '\0';
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
                fprintf(stderr, "Error: bad format sequence at end\n");
                break;
            }

            const size_t rem = sizeof(output) - (out - output);

            switch (*format) {
                case '%':
                    out += SNPRINTF(out, rem, "%%");
                    break;
                case 'a':
                    out += SNPRINTF(out, rem, "%ld", work->statuso.st_atime);
                    break;
                /* case 'A': */
                case 'b':
                    out += SNPRINTF(out, rem, "%ld", work->statuso.st_blocks);
                    break;
                case 'c':
                    out += SNPRINTF(out, rem, "%ld", work->statuso.st_ctime);
                    break;
                /* case 'C': */
                case 'd':
                    out += SNPRINTF(out, rem, "%zu", work->level);
                    break;
                case 'D':
                    out += SNPRINTF(out, rem, "%ld", work->statuso.st_dev);
                    break;
                case 'f': /* basename(work->name) */
                    out += SNPRINTF(out, rem, "%s", work->name + work->name_len - work->basename_len);
                    break;
                /* case 'F': */
                case 'g':
                    {
                        struct group *group = getgrgid(work->statuso.st_gid);
                        if (group) {
                            out += SNPRINTF(out, rem, "%s", group->gr_name);
                        }
                        else {
                            out += SNPRINTF(out, rem, "%" STAT_gid, work->statuso.st_gid);
                        }
                    }
                    break;
                case 'G':
                    out += SNPRINTF(out, rem, "%" STAT_gid, work->statuso.st_gid);
                    break;
                case 'h': /* dirname(work->name) */
                    {
                        const size_t offset = work->name_len - work->basename_len;
                        const char orig = work->name[offset];
                        work->name[offset] = '\0';
                        out += SNPRINTF(out, rem, "%s", work->name);
                        work->name[offset] = orig;
                    }
                    break;
                case 'H':
                    out += SNPRINTF(out, rem, "%s", work->orig_root.data);
                    break;
                case 'i':
                    out += SNPRINTF(out, rem, "%" STAT_ino, work->statuso.st_ino);
                    break;
                /* case 'k': */
                case 'l':
                    out += SNPRINTF(out, rem, "%s", ed->linkname);
                    break;
                case 'm':
                    out += SNPRINTF(out, rem, "%03o", work->statuso.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO));
                    break;
                case 'M':
                    {
                        char buf[11];
                        modetostr(buf, sizeof(buf), work->statuso.st_mode);
                        out += SNPRINTF(out, rem, "%s", buf);
                    }
                    break;
                case 'n':
                    out += SNPRINTF(out, rem, "%" STAT_nlink, work->statuso.st_nlink);
                    break;
                case 'p':
                    out += SNPRINTF(out, rem, "%s", work->name);
                    break;
                case 'P': /* Starting-point removed from path */
                    out += SNPRINTF(out, rem, "%s", work->name + work->orig_root.len);
                    break;
                /* case 'S': */
                case 't':
                    out += SNPRINTF(out, rem, "%ld", work->statuso.st_mtime);
                    break;
                /* case 'T': */
                case 'u':
                    {
                        struct passwd *passwd = getpwuid(work->statuso.st_uid);
                        if (passwd) {
                            out += SNPRINTF(out, rem, "%s", passwd->pw_name);
                        }
                        else {
                            out += SNPRINTF(out, rem, "%" STAT_uid, work->statuso.st_uid);
                        }
                    }
                    break;
                case 'U':
                    out += SNPRINTF(out, rem, "%" STAT_uid, work->statuso.st_uid);
                    break;
                case 'y':
                    out += SNPRINTF(out, rem, "%c", ed->type);
                    break;
                /* case 'Y': */
                /* case 'Z': */
                default:
                    out += SNPRINTF(out, rem, "%%%c", *format);
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
    struct enqueue_nondir_args *args = (struct enqueue_nondir_args *) nondir_args;

    /*
    When this function is called in processdir, descend is not called yet
    If the level is not checked here, the work->name will be printed even if
    the level is not in the range [min_level, max_level]

    Example:
    root_dir/ --- level 0
    ├── dir1/ --- level 1
    │   └── file1.txt --- level 1

    parallel_find -n 2 --min-level 1 --max-level 1
    The expected output is:
    root_dir/dir1
    root_dir/dir1/file1.txt

    But if the level is not checked here, the output will be:
    root_dir/       <- first time process_output is called in processdir before descend is called
    root_dir/dir1
    root_dir/dir1/file1.txt
    */

    if (work->level < args->pa->in.min_level) { /* work->level > max_level is handled by descend */
        return 0; /* not an error */
    }

    if (lstat_wrapper(work, 1) != 0) {
        return 1; /* error (not handled anywhere) */
    }

    const size_t user_types = args->pa->in.filter_types;
    const struct stat *st = &work->statuso;
    if (user_types) { /* if user_types was set, only process allowed types */
        if (!(((user_types & FILTER_TYPE_DIR)  && S_ISDIR(st->st_mode))  ||
              ((user_types & FILTER_TYPE_FILE) && S_ISREG(st->st_mode))  ||
              ((user_types & FILTER_TYPE_LINK) && S_ISLNK(st->st_mode)))) {
            return 0;
        }
    }
    /* if user_types was not set, process everything */

    struct PrintArgs print = {
        .output_buffer = &args->pa->obufs.buffers[args->id],
        .delim = '\0', /* not used */
        .newline = '\n',
        .mutex = args->pa->obufs.mutex,
        .outfile = args->pa->outfiles[args->id],
        .types = NULL,
        .suppress_newline = 1,
    };

    format_output(work, ed, args->pa->in.format.data, &print);

    return 0;
}

static int processdir(QPTPool_ctx_t *ctx, void *data) {
    struct PoolArgs *pa = (struct PoolArgs *) QPTPool_get_args_internal(ctx);
    struct work *work = (struct work *) data;

    int rc = 0;

    struct entry_data ed;
    ed.type = 'd';

    // TODO: update this name
    struct enqueue_nondir_args nondir_args = {
        .ctx = ctx,
        .id = QPTPool_get_id(ctx),
        .pa = pa,
    };

    process_output(work, &ed, &nondir_args);

    DIR *dir = opendir(work->name);
    if (!dir) {
        fprintf(stderr, "Error: Could not open directory \"%s\"\n", work->name);
        rc = 1;
    }
    else {
        descend(ctx, &pa->in,
                work, dir, 0, processdir,
                process_output,
                &nondir_args, NULL);
    }

    closedir(dir);
    free(work);
    return rc;
}

static void sub_help(void) {
   printf("input_dir...      walk one or more trees to search files\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        /* processing/tree walk flags */
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_FILTER_TYPE,

        /* output flags */
        FLAG_FORMAT, FLAG_OUTPUT_FILE,

        /* memory usage flags */
        FLAG_OUTPUT_BUFFER_SIZE,

        FLAG_END
    };

    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 1, "input_dir...", &pa.in);

    int rc = 0;

    if (!pa.in.format_set) {
        pa.in.format.data = (char *) DEFAULT_FORMAT;
        pa.in.format.len  = sizeof(DEFAULT_FORMAT) - 1;
        pa.in.format.free = NULL;
    }

    const str_t STDOUT_NAME = REFSTR("-", 1);

    pa.outfiles = outfiles_init((pa.in.output == STDOUT)?&STDOUT_NAME:&pa.in.outname, pa.in.maxthreads);
    if (!pa.outfiles) {
        rc = 1;
        goto cleanup_exit;
    }

    /* Initialize output buffers */
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    if (!OutputBuffers_init(&pa.obufs, pa.in.maxthreads, pa.in.output_buffer_size,
                            (pa.in.output == STDOUT)?&mutex:NULL)) {
        fprintf(stderr, "Error: Could not initialize %zu output buffers\n", pa.in.maxthreads);
        rc = 1;
        goto close_outfiles;
    }

    /* create a queue per thread pool */
    QPTPool_ctx_t *ctx = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        rc = 1;
        goto cleanup_qptp;
    }

    for (int i = idx; i < argc; i++) {
        const char *path = argv[i];
        const size_t path_len = strlen(path);

        struct work *work = new_work_with_name(NULL, 0, path, path_len);
        if (!work) {
            fprintf(stderr, "Error: Failed to create work item for \"%s\"\n", path);
            rc = 1;
            continue;
        }

        work->orig_root.data = (char *) path;
        work->orig_root.len = path_len + 1;  /* add 1 to remove the slash added from descend */

        /* input path that are links are followed (find -H) */
        if (stat(path, &work->statuso) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Cannot stat \"%s\": %s (%d)\n",
                    path, strerror(err), err);
            free(work);
            rc = 1;
            continue;
        }

        if (S_ISREG(work->statuso.st_mode)) {
            struct enqueue_nondir_args qptp_vals = {
                .ctx = ctx,
                .id = 0,
                .pa = &pa,
            };
            process_output(work, NULL, &qptp_vals);
            free(work);
        }
        else if (S_ISDIR(work->statuso.st_mode)) {
            QPTPool_enqueue(ctx, processdir, work);
        }
        else {
            fprintf(stderr, "Error: Unsupported type for \"%s\"\n", work->name);
            free(work);
            rc = 1;
        }
    }

    QPTPool_stop(ctx);

    const uint64_t threads_started   = QPTPool_threads_started(ctx);
    const uint64_t threads_completed = QPTPool_threads_completed(ctx);

  cleanup_qptp:
    QPTPool_destroy(ctx);

    OutputBuffers_flush_to_multiple(&pa.obufs, pa.outfiles);
    OutputBuffers_destroy(&pa.obufs);

  close_outfiles:
    outfiles_fin(pa.outfiles, pa.in.maxthreads);

  cleanup_exit:
    input_fini(&pa.in);

    return ((rc == 0) && (threads_started == threads_completed)) ? EXIT_SUCCESS : EXIT_FAILURE;
}
