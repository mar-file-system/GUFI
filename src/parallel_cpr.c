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
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "utils.h"

typedef struct str {
    char *data;
    size_t len;
} str_t;

static str_t create_dst_name(struct input *in, struct work *work) {
    str_t dst;

    dst.len = in->nameto.len + 1 + work->name_len - work->root_parent.len;
    dst.data = malloc(dst.len + 1);

    SNFORMAT_S(dst.data, dst.len + 1, 3,
               in->nameto.data, in->nameto.len,
               "/", (size_t) 1,
               work->name + work->root_parent.len, work->name_len - work->root_parent.len);

    return dst; /* return by value instead of returning newly allocated pointer that needs to be cleaned up */
}

static int set_xattrs(const char *name, struct xattrs *xattrs) {
    int rc = 0;
    for(size_t i = 0; i < xattrs->count; i++) {
        struct xattr *xattr = &xattrs->pairs[i];

        if (SETXATTR(name, xattr->name, xattr->value, xattr->value_len) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not set xattr %s=%s on \"%s\": %s (%d)\n",
                    xattr->name, xattr->value, name, strerror(err), err);
            rc = 1;
        }
    }
    return rc;
}

struct work_data {
    struct work work;
    struct entry_data ed;
};

#define print_error_and_goto(msg, path, label)      \
    const int err = errno;                          \
    fprintf(stderr, "Error: %s \"%s\": %s (%d)\n",  \
            msg, path, strerror(err), err);         \
    rc = 1;                                         \
    goto label

/* copy a single file in a single thread */
/* TODO: if file is too big, split up copy */
static int cpr_file(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    (void) ctx;
    (void) id;

    struct input *in = (struct input *) args;
    struct work_data *wd = (struct work_data *) data;
    struct work *work = &wd->work;
    struct entry_data *ed = &wd->ed;

    int rc = 0;

    struct stat *st = &ed->statuso;
    struct stat st_stack;
    if (!ed->lstat_called) {
        if (lstat(work->name, &st_stack) != 0) {
            print_error_and_goto("Could not lstat file", work->name, cleanup);
        }
        st = &st_stack;
    }

    const int src_fd = open(work->name, O_RDONLY);
    if (src_fd < 0) {
        print_error_and_goto("Could not open file", work->name, cleanup);
    }

    str_t dst = create_dst_name(in, work);

    const int dst_fd = open(dst.data, O_CREAT | O_WRONLY | O_TRUNC, st->st_mode);
    if (dst_fd < 0) {
        print_error_and_goto("Could not open file", dst.data, free_name);
    }

    if (copyfd(src_fd, 0, dst_fd, 0, st->st_size) != st->st_size) {
        print_error_and_goto("Could not copy file", work->name, free_name);
    }

    rc = set_xattrs(dst.data, &ed->xattrs);
    xattrs_cleanup(&ed->xattrs);

    close(dst_fd);

  free_name:
    free(dst.data);

    close(src_fd);

  cleanup:
    free(wd);

    return rc;
}

static int cpr_link(struct work *work, struct entry_data *ed, struct input *in) {
    int rc = 0;

    /* need to give users ability to force overwriting of links */

    str_t dst = create_dst_name(in, work);

    if (symlink(ed->linkname, dst.data) != 0) {
        print_error_and_goto("Could not create link", dst.data, cleanup);
    }

  cleanup:
    free(dst.data);

    return rc;
}

struct QPTPool_vals {
    QPTPool_t *ctx;
    size_t id;
    struct input *in;
};

/* enqueue nondirs instead of copying them right here because they might be enormous */
static int enqueue_nondir(struct work *work, struct entry_data *ed, void *nondir_args) {
    struct QPTPool_vals *args = (struct QPTPool_vals *) nondir_args;

    if (S_ISREG(ed->statuso.st_mode)) {
        struct work_data *wd = calloc(1, sizeof(*wd) + work->name_len + 1);
        memcpy(&wd->work, work, sizeof(wd->work));
        wd->work.name = (char *) &wd[1];
        memcpy(wd->work.name, work->name, work->name_len);
        xattrs_setup(&wd->ed.xattrs);
        memcpy(&wd->ed, ed, sizeof(*ed));

        if (args->in->process_xattrs) {
            wd->ed.xattrs.pairs = NULL;
            xattrs_alloc(&wd->ed.xattrs);
            memcpy(wd->ed.xattrs.pairs, ed->xattrs.pairs, ed->xattrs.count * sizeof(struct xattr));
        }

        QPTPool_enqueue(args->ctx, args->id, cpr_file, wd);
    }
    else if (S_ISLNK(ed->statuso.st_mode)) {
        /* copy right here since symlinks should not take much time to copy */
        cpr_link(work, ed, args->in);
    }

    return 0;
}

/* set up the copied directory and enqueue per-file copy threads */
static int cpr_dir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct input *in = (struct input *) args;
    struct work *work = (struct work *) data;

    int rc = 0;

    DIR *dir = opendir(work->name);
    if (!dir) {
        print_error_and_goto("Could not open_directory", work->name, cleanup);
    }

    struct stat st;
    if (lstat(work->name, &st) != 0) {
        print_error_and_goto("Could not lstat directory", work->name, cleanup);
    }

    str_t dst = create_dst_name(in, work);

    /* ensure parent directory exists before processing children */
    if (mkdir(dst.data, st.st_mode & 0777) != 0) {
        if (errno != EEXIST) {
            print_error_and_goto("Could not create directory", dst.data, free_dst);
        }
    }

    if (chown(dst.data, st.st_uid, st.st_gid) != 0) {
        print_error_and_goto("Could not chown directory", dst.data, free_dst);
    }

    struct QPTPool_vals qptp_vals = {
        .ctx = ctx,
        .id = id,
        .in = in,
    };

    /* process children */
    descend(ctx, id, args, in,
            work, st.st_ino, dir, 0,
            cpr_dir,
            enqueue_nondir, &qptp_vals,
            NULL);

    if (in->process_xattrs) {
        struct xattrs xattrs;
        xattrs_setup(&xattrs);
        xattrs_get(work->name, &xattrs);
        rc = set_xattrs(dst.data, &xattrs);
        xattrs_cleanup(&xattrs);
    }

  free_dst:
    free(dst.data);

  cleanup:
    closedir(dir);
    free(work);

    return rc;
}

static int setup(const refstr_t *dst) {
    struct stat st;
    st.st_mode = umask(0);
    st.st_uid = geteuid();
    st.st_gid = getegid();

    umask(st.st_mode); /* reset umask */

    st.st_mode = ~st.st_mode & 0777;

    return dupdir(dst->data, &st);
}

static void sub_help(void) {
   printf("src...     directory to copy\n");
   printf("dst        directory to place sources under\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    struct input in;
    process_args_and_maybe_exit("hHvn:x", 2, "src... dst", &in);

    /* parse positional args, following the options */
    /* does not have to be canonicalized */
    INSTALL_STR(&in.nameto, argv[argc - 1]);

    int rc = setup(&in.nameto);
    if (rc != 0) {
        fprintf(stderr, "Error: Cannot copy to \"%s\": %s (%d)\n",
                in.nameto.data, strerror(rc), rc);
        input_fini(&in);
        return EXIT_FAILURE;
    }

    /* get realpath after set up to ensure the path exists */
    const char *dst = in.nameto.data;
    const char *real_dst = realpath(dst, NULL);
    if (!real_dst) {
        const int err = errno;
        fprintf(stderr, "Error: Cannot get realpath of \"%s\": %s (%d)\n",
                dst, strerror(err), err);
        input_fini(&in);
        return EXIT_FAILURE;
    }

    QPTPool_t *pool = QPTPool_init(in.maxthreads, &in);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        free((char *) real_dst);
        input_fini(&in);
        return EXIT_FAILURE;
    }

    argc--;
    for(int i = idx; i < argc; i++) {
        const char *path = argv[i];

        struct stat st;
        if (lstat(path, &st) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Cannot lstat \"%s\": %s (%d)\n",
                    path, strerror(err), err);
            rc = 1;
            continue;
        }

        struct work *work = new_work_with_name(NULL, 0, path, strlen(path));

        /* remove trailing slashes (+ 1 to keep at least 1 character) */
        const size_t len = strlen(path);
        work->name_len = trailing_non_match_index(path + 1, len - 1, "/", 1) + 1;
        work->name[work->name_len] = '\0';
        work->basename_len = work->name_len - trailing_match_index(work->name, work->name_len, "/", 1);
        work->root_parent.data = path;
        work->root_parent.len = dirname_len(path, work->name_len);

        if (S_ISDIR(st.st_mode)) {
            const char *real_path = realpath(path, NULL);
            const int match = !strncmp(real_dst, real_path, strlen(real_path));
            free((char *) real_path);
            if (match) {
                fprintf(stderr, "Not copying into itself: \"%s\" -> \"%s\"\n",
                        path, dst);
                rc = 1;
                continue;
            }

            QPTPool_enqueue(pool, 0, cpr_dir, work);
        }
        else if (S_ISLNK(st.st_mode)) {
            struct entry_data ed;

            /* see man 2 readlink */
            const ssize_t link_len = readlink(work->name, ed.linkname, sizeof(ed.linkname));
            if ((link_len < 0) || (link_len == sizeof(ed.linkname))) {
                const int err = errno;
                fprintf(stderr, "Error: Could not read link \"%s\": %s (%d)\n",
                        work->name, strerror(err), err);
                rc = 1;
            }
            else {
                ed.linkname[link_len] = '\0';

                /* copy right here instead of enqueuing */
                cpr_link(work, &ed, &in);
            }

            free_work(work);
        }
        else if (S_ISREG(st.st_mode)) {
            struct work_data *wd = calloc(1, sizeof(*wd) + work->name_len + 1);
            memcpy(&wd->work, work, sizeof(wd->work));
            wd->work.name = (char *) &wd[1];
            memcpy(wd->work.name, work->name, work->name_len);

            xattrs_setup(&wd->ed.xattrs);

            if (in.process_xattrs) {
                xattrs_get(work->name, &wd->ed.xattrs);
            }

            QPTPool_enqueue(pool, 0, cpr_file, wd);

            free_work(work);
        }
        else {
            free(work);
            fprintf(stderr, "Not copying \"%s\"\n", path);
            rc = 1;
        }
    }

    QPTPool_stop(pool);
    const uint64_t threads_started   = QPTPool_threads_started(pool);
    const uint64_t threads_completed = QPTPool_threads_completed(pool);
    QPTPool_destroy(pool);

    free((char *) real_dst);

    input_fini(&in);

    return ((rc == 0) && (threads_started == threads_completed))?EXIT_SUCCESS:EXIT_FAILURE;
}
