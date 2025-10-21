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



#include <gtest/gtest.h>

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <cstdio>
#include <unistd.h>

#include "config.h"
#include "trace.h"

static const char delim = '\x1e';

static const struct xattr EXPECTED_XATTR[] = {
    {
        "xattr.key0", 10,
        "xattr.val0", 10,
    },
    {
        "xattr.key1", 10,
        "xattr.val1", 10,
    },
};

static struct xattrs EXPECTED_XATTRS = {
    (struct xattr *) EXPECTED_XATTR,
    0,
    0,
    2,
};

static const char EXPECTED_XATTRS_STR[] = "xattr.key0\x1fxattr.val0\x1f"
                                          "xattr.key1\x1fxattr.val1\x1f";
static const std::size_t EXPECTED_XATTRS_STR_LEN = sizeof(EXPECTED_XATTRS_STR) - 1;

static struct work *get_work(struct entry_data *ed) {
    struct work *new_work        = new_work_with_name(nullptr, 0, "name", 4);
    ed->type                     = 't';
    snprintf(ed->linkname, sizeof(ed->linkname), "link");
    new_work->statuso.st_ino     = 0xfedc;
    new_work->statuso.st_mode    = 0777;
    new_work->statuso.st_nlink   = 2048;
    new_work->statuso.st_uid     = 1001;
    new_work->statuso.st_gid     = 1001;
    new_work->statuso.st_size    = 4096;
    new_work->statuso.st_blksize = 4096;
    new_work->statuso.st_blocks  = 1;
    new_work->statuso.st_atime   = 0x1234;
    new_work->statuso.st_mtime   = 0x5678;
    new_work->statuso.st_ctime   = 0x9abc;
    ed->xattrs.pairs             = (struct xattr *) EXPECTED_XATTR;
    ed->xattrs.name_len          = 0;
    ed->xattrs.len               = 0;
    ed->xattrs.count             = 2;
    new_work->crtime             = 0x9abc;
    ed->ossint1                  = 1;
    ed->ossint2                  = 2;
    ed->ossint3                  = 3;
    ed->ossint4                  = 4;
    snprintf(ed->osstext1, sizeof(ed->osstext1), "osstext1");
    snprintf(ed->osstext2, sizeof(ed->osstext2), "osstext2");
    new_work->pinode           = 0xdef0;

    return new_work;
}

static int to_string(char *line, const std::size_t size, struct work *work, struct entry_data *ed) {
    const int part1 = snprintf(line, size,
                               "%s%c"
                               "%c%c"
                               "%" STAT_ino "%c"
                               "%d%c"
                               "%" STAT_nlink"%c"
                               "%d%c"
                               "%d%c"
                               "%" STAT_size "%c"
                               "%" STAT_bsize "%c"
                               "%" STAT_blocks "%c"
                               "%ld%c"
                               "%ld%c"
                               "%ld%c"
                               "%s%c",
                               work->name,               delim,
                               ed->type,                 delim,
                               work->statuso.st_ino,     delim,
                               work->statuso.st_mode,    delim,
                               work->statuso.st_nlink,   delim,
                               work->statuso.st_uid,     delim,
                               work->statuso.st_gid,     delim,
                               work->statuso.st_size,    delim,
                               work->statuso.st_blksize, delim,
                               work->statuso.st_blocks,  delim,
                               work->statuso.st_atime,   delim,
                               work->statuso.st_mtime,   delim,
                               work->statuso.st_ctime,   delim,
                               ed->linkname,             delim);

    line += part1;

    memcpy(line, EXPECTED_XATTRS_STR, EXPECTED_XATTRS_STR_LEN);
    line += EXPECTED_XATTRS_STR_LEN;

    const int part2 = snprintf(line, size,
                               "%c"
                               "%ld%c"
                               "%d%c"
                               "%d%c"
                               "%d%c"
                               "%d%c"
                               "%s%c"
                               "%s%c"
                               "%lld%c"
                               "\n",
                                             delim,
                               work->crtime, delim,
                               ed->ossint1,  delim,
                               ed->ossint2,  delim,
                               ed->ossint3,  delim,
                               ed->ossint4,  delim,
                               ed->osstext1, delim,
                               ed->osstext2, delim,
                               work->pinode, delim);

    line += part2;
    *line = '\0';

    return part1 + EXPECTED_XATTRS_STR_LEN + part2;
}

TEST(trace, worktofile) {
    struct entry_data ed;
    struct work *work = get_work(&ed);

    // write a known struct to a file
    char buf[4096];
    FILE *file = fmemopen(buf, sizeof(buf), "w");
    ASSERT_NE(file, nullptr);

    const int written = worktofile(file, delim, 0, work, &ed);
    fclose(file);

    ASSERT_GT(written, 0);
    ASSERT_LT(written, (int) sizeof(buf));

    // generate the string to compare with
    char line[4096];
    const int rc = to_string(line, sizeof(line), work, &ed);

    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    EXPECT_STREQ(buf, line);

    EXPECT_EQ(worktofile(nullptr, delim, 0, work, &ed),  -1);
    EXPECT_EQ(worktofile(file, delim, 0, nullptr,  &ed), -1);
    EXPECT_EQ(worktofile(file, delim, 0, work, nullptr), -1);

    free(work);
}

TEST(trace, worktobuffer) {
    struct entry_data ed;
    struct work *work = get_work(&ed);

    // write a known struct to a file
    char *buf = NULL;
    std::size_t size = 0;
    std::size_t offset = 0;

    const int written = worktobuffer(&buf, &size, &offset, delim, 0, work, &ed);
    ASSERT_GT(written, 0);
    ASSERT_NE(buf,     nullptr);
    EXPECT_GT(size,    (std::size_t) 0);
    EXPECT_GT(offset,  (std::size_t) 0);

    // generate the string to compare with
    char line[4096];
    const int rc = to_string(line, sizeof(line), work, &ed);

    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    EXPECT_STREQ(buf, line);

    EXPECT_EQ(worktobuffer(nullptr, &size,   &offset, delim, 0, work,    &ed),     -1);
    EXPECT_EQ(worktobuffer(&buf,    nullptr, &offset, delim, 0, work,    &ed),     -1);
    EXPECT_EQ(worktobuffer(&buf,    &size,   nullptr, delim, 0, work,    &ed),     -1);
    EXPECT_EQ(worktobuffer(&buf,    &size,   &offset, delim, 0, nullptr, &ed),     -1);
    EXPECT_EQ(worktobuffer(&buf,    &size,   &offset, delim, 0, work,    nullptr), -1);

    free(buf);
    free(work);
}

#define COMPARE(src, src_ed, dst, dst_ed)                               \
    EXPECT_STREQ(dst->name,              src->name);                    \
    EXPECT_EQ(dst_ed.type,               src_ed.type);                  \
    EXPECT_EQ(dst->statuso.st_ino,       src->statuso.st_ino);          \
    EXPECT_EQ(dst->statuso.st_mode,      src->statuso.st_mode);         \
    EXPECT_EQ(dst->statuso.st_nlink,     src->statuso.st_nlink);        \
    EXPECT_EQ(dst->statuso.st_uid,       src->statuso.st_uid);          \
    EXPECT_EQ(dst->statuso.st_gid,       src->statuso.st_gid);          \
    EXPECT_EQ(dst->statuso.st_size,      src->statuso.st_size);         \
    EXPECT_EQ(dst->statuso.st_blksize,   src->statuso.st_blksize);      \
    EXPECT_EQ(dst->statuso.st_blocks,    src->statuso.st_blocks);       \
    EXPECT_EQ(dst->statuso.st_atime,     src->statuso.st_atime);        \
    EXPECT_EQ(dst->statuso.st_mtime,     src->statuso.st_mtime);        \
    EXPECT_EQ(dst->statuso.st_ctime,     src->statuso.st_ctime);        \
    EXPECT_STREQ(dst_ed.linkname,        src_ed.linkname);              \
    EXPECT_EQ(dst->crtime,               src->crtime);                  \
    EXPECT_EQ(dst_ed.ossint1,            src_ed.ossint1);               \
    EXPECT_EQ(dst_ed.ossint2,            src_ed.ossint2);               \
    EXPECT_EQ(dst_ed.ossint3,            src_ed.ossint3);               \
    EXPECT_EQ(dst_ed.ossint4,            src_ed.ossint4);               \
    EXPECT_STREQ(dst_ed.osstext1,        src_ed.osstext1);              \
    EXPECT_STREQ(dst_ed.osstext2,        src_ed.osstext2);              \
    EXPECT_EQ(dst->pinode,               src->pinode);                  \

TEST(trace, linetowork) {
    struct entry_data src_ed;
    struct work *src = get_work(&src_ed);

    // write the known struct to a string using an alternative write function
    char line[4096];
    const int rc = to_string(line, sizeof(line), src, &src_ed);
    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    // read the string
    struct work *work;
    struct entry_data ed;
    EXPECT_EQ(linetowork(line, rc, delim, &work, &ed), 0);

    COMPARE(src, src_ed, work, ed);

    EXPECT_STREQ(ed.xattrs.pairs[0].name,  EXPECTED_XATTRS.pairs[0].name);
    EXPECT_STREQ(ed.xattrs.pairs[0].value, EXPECTED_XATTRS.pairs[0].value);
    EXPECT_STREQ(ed.xattrs.pairs[1].name,  EXPECTED_XATTRS.pairs[1].name);
    EXPECT_STREQ(ed.xattrs.pairs[1].value, EXPECTED_XATTRS.pairs[1].value);

    xattrs_cleanup(&ed.xattrs);
    free(work);
    free(src);

    EXPECT_EQ(linetowork(nullptr, rc, delim, &work, &ed),  -1);
    EXPECT_EQ(linetowork(line, rc, delim, nullptr,  &ed),  -1);
    EXPECT_EQ(linetowork(line, rc, delim, &work, nullptr), -1);
}

TEST(open_traces, too_many) {
    EXPECT_EQ(open_traces(nullptr, (size_t) -1), nullptr);
}

TEST(scout, trace) {
    struct entry_data src_ed;
    struct work *src = get_work(&src_ed);
    src_ed.type = 'd';

    // write the known struct to a string using an alternative write function
    char line[4096];
    const int rc = to_string(line, sizeof(line), src, &src_ed);
    free(src);
    ASSERT_GT(rc, -1);
    const std::size_t len = strlen(line);
    ASSERT_EQ(rc, (int) len);

    // write trace to file
    char tracename[] = "XXXXXX";
    const int fd = mkstemp(tracename);
    ASSERT_GT(fd, -1);
    ASSERT_EQ(write(fd, line, len), (ssize_t) len);

    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    struct TraceStats stats = {};
    stats.mutex = &mutex;
    stats.remaining = 2; // run twice to cover both sides of final print condition

    struct ScoutTraceArgs sta;
    sta.delim      = delim;
    sta.tracename  = tracename;
    sta.tr.fd      = fd;
    sta.tr.start   = 0;
    sta.tr.end     = len;
    sta.processdir = [] (QPTPool_t *, const std::size_t, void *data, void *) {
        struct row *row = (struct row *) data;
        row_destroy(&row);
        return 0;
    };
    sta.free       = nullptr;
    sta.stats      = &stats;

    QPTPool_t *pool = QPTPool_init(1, nullptr);
    ASSERT_NE(pool, nullptr);
    #ifdef QPTPOOL_SWAP
    EXPECT_EQ(QPTPool_set_queue_limit(pool, 1), 0); // if the WAIT queue already has 1 item, swap out the new item
    #endif
    ASSERT_EQ(QPTPool_start(pool), 0);

    struct Args {
        pthread_mutex_t mutex;
        pthread_cond_t cond;
        bool running;
    };

    Args args;
    args.mutex = PTHREAD_MUTEX_INITIALIZER;
    args.cond = PTHREAD_COND_INITIALIZER;
    args.running = false;

    // enqueue stuck thread
    pthread_mutex_lock(&args.mutex);
    QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT,
                         [](QPTPool_t *, const std::size_t, void *data, void *) -> int {
                             Args *args = (Args *) data;

                             // alert the main thread that this thread has started
                             // (has been removed from the WAIT queue)
                             pthread_mutex_lock(&args->mutex);
                             args->running = true;
                             pthread_cond_broadcast(&args->cond);

                             // block until main thread wants to stop this thread
                             while (args->running) {
                                 pthread_cond_wait(&args->cond, &args->mutex);
                             }
                             pthread_mutex_unlock(&args->mutex);
                             return 0;
                         }, &args
                         #ifdef QPTPOOL_SWAP
                         , nullptr, nullptr
                         #endif
        );

    // wait until the first thread starts
    while (!args.running) {
        pthread_cond_wait(&args.cond, &args.mutex);
    }
    pthread_mutex_unlock(&args.mutex);

    // enqueue item into WAIT queue
    QPTPool_enqueue_here(pool, 0, QPTPool_enqueue_WAIT,
                         [](QPTPool_t *, const std::size_t, void *, void *) -> int {
                             // no-op
                             return 0;
                         }, nullptr
                         #ifdef QPTPOOL_SWAP
                         , nullptr, nullptr
                         #endif
        );

    // not enqueuing scout_trace
    // struct (on stack) is not cleaned up, so should not throw
    EXPECT_EQ(scout_trace(pool, 0, &sta, nullptr), 0);

    // allow the first thread to complete
    pthread_mutex_lock(&args.mutex);
    args.running = false;
    pthread_cond_broadcast(&args.cond);
    pthread_mutex_unlock(&args.mutex);

    EXPECT_EQ(scout_trace(pool, 0, &sta, nullptr), 0);

    QPTPool_stop(pool);
    #ifdef QPTPOOL_SWAP
    EXPECT_GT(QPTPool_threads_started(pool),   (uint64_t) 4);
    EXPECT_GT(QPTPool_threads_completed(pool), (uint64_t) 4);
    #else
    EXPECT_EQ(QPTPool_threads_started(pool),   (uint64_t) 4);
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 4);
    #endif
    QPTPool_destroy(pool);

    EXPECT_EQ(stats.remaining,  (size_t)   0);
    EXPECT_GT(stats.thread_time,(uint64_t) 0);
    EXPECT_EQ(stats.files,      (size_t)   0);
    EXPECT_EQ(stats.dirs,       (size_t)   2);
    EXPECT_EQ(stats.empty,      (size_t)   2);

    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(tracename), 0);
}

TEST(enqueue_traces, bad) {
    char tracename[] = "trace";
    char *tracenameptr = tracename;
    int tracefd = -1;
    struct TraceStats stats = {};

    EXPECT_EQ(enqueue_traces(&tracenameptr, &tracefd, 1, '|', 10, nullptr, nullptr, &stats), (std::size_t) 0);
}

TEST(scout, stream) {
    struct entry_data src_ed;
    struct work *src = get_work(&src_ed);
    src_ed.type = 'd';

    // write the known struct to a string using an alternative write function
    char line[4096];
    const int rc = to_string(line, sizeof(line), src, &src_ed);
    free(src);
    ASSERT_GT(rc, -1);
    const std::size_t len = strlen(line);
    ASSERT_EQ(rc, (int) len);

    // write trace to pipe
    int fds[2];
    ASSERT_EQ(pipe(fds),0);
    ASSERT_EQ(write(fds[1], line, len), (ssize_t) len);
    EXPECT_EQ(close(fds[1]), 0);

    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    struct TraceStats stats = {};
    stats.mutex = &mutex;

    /* malloc sta because normally free branch is not triggered */
    struct ScoutTraceArgs *sta = (struct ScoutTraceArgs *) malloc(sizeof(*sta));
    sta->delim      = delim;
    sta->tracename  = "-";
    sta->tr.fd      = fds[0];
    sta->tr.start   = 0;
    sta->tr.end     = len;
    sta->processdir = [] (QPTPool_t *, const std::size_t, void *data, void *) {
        struct row *row = (struct row *) data;
        row_destroy(&row);
        return 0;
    };
    sta->free       = free;
    sta->stats      = &stats;

    QPTPool_t *pool = QPTPool_init(1, nullptr);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(QPTPool_start(pool), 0);

    EXPECT_NE(QPTPool_enqueue(pool, 0, scout_stream, sta), QPTPool_enqueue_ERROR);

    QPTPool_stop(pool);
    EXPECT_EQ(QPTPool_threads_started(pool),   (uint64_t) 2);
    EXPECT_EQ(QPTPool_threads_completed(pool), (uint64_t) 2);
    QPTPool_destroy(pool);

    /* stats.remaining not modified */
    EXPECT_GT(stats.thread_time,(uint64_t) 0);
    EXPECT_EQ(stats.files,      (size_t)   0);
    EXPECT_EQ(stats.dirs,       (size_t)   1);
    EXPECT_EQ(stats.empty,      (size_t)   1);

    EXPECT_EQ(close(fds[0]), 0);
}
