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



#include <climits>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <random>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "bf.h"
#include "utils.h"

TEST(SNPRINTF, bad) {
    char buf[1];
    const std::size_t size = sizeof(buf);
    EXPECT_EQ(SNPRINTF(buf, size, "%s", "A"), (ssize_t) size);
}

TEST(SNFORMAT_S, concatenate) {
    char buf[1024];
    SNFORMAT_S(buf, 1024, 3, "A", (size_t) 1, "BC", (size_t) 2, "DEF", (size_t) 3);
    EXPECT_STREQ(buf, "ABCDEF");
}

TEST(SNFORMAT_S, fill_buffer) {
    char buf[1024];

    EXPECT_EQ(SNFORMAT_S(buf, 10, 1, "0123456789", (size_t) 10), (size_t) 9);
    EXPECT_STREQ(buf, "012345678");
    EXPECT_EQ(buf[9], '\0');

    EXPECT_EQ(SNFORMAT_S(buf, 5, 1, "0123456789", (size_t) 10), (size_t) 4);
    EXPECT_STREQ(buf, "0123");
    EXPECT_EQ(buf[4], '\0');
}

TEST(get, queue_limit) {
    EXPECT_EQ(get_queue_limit(0,                0), (uint64_t) 0); // not set
    EXPECT_EQ(get_queue_limit(0,                1), (uint64_t) 0); // not set
    EXPECT_EQ(get_queue_limit(1 * sizeof(work), 1), (uint64_t) 1); // max(1, 1)
    EXPECT_EQ(get_queue_limit(1 * sizeof(work), 2), (uint64_t) 1); // max(0, 1)
    EXPECT_EQ(get_queue_limit(2 * sizeof(work), 1), (uint64_t) 2); // max(2, 1)
}

TEST(summary, zeroit) {
    struct sum summary;
    ASSERT_EQ(zeroit(&summary), 0);

    EXPECT_EQ(summary.totfiles, 0);
    EXPECT_EQ(summary.totlinks, 0);
    EXPECT_EQ(summary.minuid, LLONG_MAX);
    EXPECT_EQ(summary.maxuid, LLONG_MIN);
    EXPECT_EQ(summary.mingid, LLONG_MAX);
    EXPECT_EQ(summary.maxgid, LLONG_MIN);
    EXPECT_EQ(summary.minsize, LLONG_MAX);
    EXPECT_EQ(summary.maxsize, LLONG_MIN);
    EXPECT_EQ(summary.totltk, 0);
    EXPECT_EQ(summary.totmtk, 0);
    EXPECT_EQ(summary.totltm, 0);
    EXPECT_EQ(summary.totmtm, 0);
    EXPECT_EQ(summary.totmtg, 0);
    EXPECT_EQ(summary.totmtt, 0);
    EXPECT_EQ(summary.totsize, 0);
    EXPECT_EQ(summary.minctime, LLONG_MAX);
    EXPECT_EQ(summary.maxctime, LLONG_MIN);
    EXPECT_EQ(summary.minmtime, LLONG_MAX);
    EXPECT_EQ(summary.maxmtime, LLONG_MIN);
    EXPECT_EQ(summary.minatime, LLONG_MAX);
    EXPECT_EQ(summary.maxatime, LLONG_MIN);
    EXPECT_EQ(summary.minblocks, LLONG_MAX);
    EXPECT_EQ(summary.maxblocks, LLONG_MIN);
    EXPECT_EQ(summary.totxattr, 0);
    EXPECT_EQ(summary.totsubdirs, 0);
    EXPECT_EQ(summary.maxsubdirfiles, LLONG_MIN);
    EXPECT_EQ(summary.maxsubdirlinks, LLONG_MIN);
    EXPECT_EQ(summary.maxsubdirsize, LLONG_MIN);
    EXPECT_EQ(summary.mincrtime, LLONG_MAX);
    EXPECT_EQ(summary.maxcrtime, LLONG_MIN);
    EXPECT_EQ(summary.minossint1, LLONG_MAX);
    EXPECT_EQ(summary.maxossint1, LLONG_MIN);
    EXPECT_EQ(summary.totossint1, 0);
    EXPECT_EQ(summary.minossint2, LLONG_MAX);
    EXPECT_EQ(summary.maxossint2, LLONG_MIN);
    EXPECT_EQ(summary.totossint2, 0);
    EXPECT_EQ(summary.minossint3, LLONG_MAX);
    EXPECT_EQ(summary.maxossint3, LLONG_MIN);
    EXPECT_EQ(summary.totossint3, 0);
    EXPECT_EQ(summary.minossint4, LLONG_MAX);
    EXPECT_EQ(summary.maxossint4, LLONG_MIN);
    EXPECT_EQ(summary.totossint4, 0);
}

TEST(summary, sumit_file) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution <> dist(1, INT_MAX);

    struct sum summary;
    ASSERT_EQ(zeroit(&summary), 0);

    struct work *pwork = new_work_with_name(NULL, 0, NULL, 0);
    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));

    ed.type              = 'f';
    ed.statuso.st_uid    = dist(gen);
    ed.statuso.st_gid    = dist(gen);
    ed.statuso.st_size   = dist(gen);
    ed.statuso.st_ctime  = dist(gen);
    ed.statuso.st_mtime  = dist(gen);
    ed.statuso.st_atime  = dist(gen);
    ed.statuso.st_blocks = dist(gen);
    pwork->pinode        = dist(gen);
    ed.crtime            = dist(gen);
    ed.ossint1           = dist(gen);
    ed.ossint2           = dist(gen);
    ed.ossint3           = dist(gen);
    ed.ossint4           = dist(gen);

    ASSERT_EQ(sumit(&summary, &ed), 0);

    EXPECT_EQ(summary.totfiles,   1);
    EXPECT_EQ(summary.totlinks,   0);
    EXPECT_EQ(summary.minblocks,  ed.statuso.st_blocks);
    EXPECT_EQ(summary.maxblocks,  ed.statuso.st_blocks);
    EXPECT_EQ(summary.minuid,     ed.statuso.st_uid);
    EXPECT_EQ(summary.maxuid,     ed.statuso.st_uid);
    EXPECT_EQ(summary.mingid,     ed.statuso.st_gid);
    EXPECT_EQ(summary.maxgid,     ed.statuso.st_gid);
    EXPECT_EQ(summary.minsize,    ed.statuso.st_size);
    EXPECT_EQ(summary.maxsize,    ed.statuso.st_size);
    EXPECT_EQ(summary.minctime,   ed.statuso.st_ctime);
    EXPECT_EQ(summary.maxctime,   ed.statuso.st_ctime);
    EXPECT_EQ(summary.minmtime,   ed.statuso.st_mtime);
    EXPECT_EQ(summary.maxmtime,   ed.statuso.st_mtime);
    EXPECT_EQ(summary.minatime,   ed.statuso.st_atime);
    EXPECT_EQ(summary.maxatime,   ed.statuso.st_atime);
    EXPECT_EQ(summary.mincrtime,  ed.crtime);
    EXPECT_EQ(summary.maxcrtime,  ed.crtime);
    EXPECT_EQ(summary.minossint1, ed.ossint1);
    EXPECT_EQ(summary.maxossint1, ed.ossint1);
    EXPECT_EQ(summary.minossint2, ed.ossint2);
    EXPECT_EQ(summary.maxossint2, ed.ossint2);
    EXPECT_EQ(summary.minossint3, ed.ossint3);
    EXPECT_EQ(summary.maxossint3, ed.ossint3);
    EXPECT_EQ(summary.minossint4, ed.ossint4);
    EXPECT_EQ(summary.maxossint4, ed.ossint4);
    EXPECT_EQ(summary.totxattr,   0);

    free(pwork);
}

TEST(summary, sumit_link) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution <> dist(1, INT_MAX);

    struct sum summary;
    ASSERT_EQ(zeroit(&summary), 0);

    struct work *pwork = new_work_with_name(NULL, 0, NULL, 0);
    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));

    ed.type              = 'l';
    ed.statuso.st_uid    = dist(gen);
    ed.statuso.st_gid    = dist(gen);
    ed.statuso.st_size   = dist(gen);
    ed.statuso.st_ctime  = dist(gen);
    ed.statuso.st_mtime  = dist(gen);
    ed.statuso.st_atime  = dist(gen);
    ed.statuso.st_blocks = dist(gen);
    pwork->pinode        = dist(gen);
    ed.crtime            = dist(gen);
    ed.ossint1           = dist(gen);
    ed.ossint2           = dist(gen);
    ed.ossint3           = dist(gen);
    ed.ossint4           = dist(gen);

    ASSERT_EQ(sumit(&summary, &ed), 0);

    EXPECT_EQ(summary.totfiles,   0);
    EXPECT_EQ(summary.totlinks,   1);
    EXPECT_EQ(summary.minblocks,  LLONG_MAX);
    EXPECT_EQ(summary.maxblocks,  LLONG_MIN);
    EXPECT_EQ(summary.minuid,     ed.statuso.st_uid);
    EXPECT_EQ(summary.maxuid,     ed.statuso.st_uid);
    EXPECT_EQ(summary.mingid,     ed.statuso.st_gid);
    EXPECT_EQ(summary.maxgid,     ed.statuso.st_gid);
    EXPECT_EQ(summary.minsize,    LLONG_MAX);
    EXPECT_EQ(summary.maxsize,    LLONG_MIN);
    EXPECT_EQ(summary.minctime,   ed.statuso.st_ctime);
    EXPECT_EQ(summary.maxctime,   ed.statuso.st_ctime);
    EXPECT_EQ(summary.minmtime,   ed.statuso.st_mtime);
    EXPECT_EQ(summary.maxmtime,   ed.statuso.st_mtime);
    EXPECT_EQ(summary.minatime,   ed.statuso.st_atime);
    EXPECT_EQ(summary.maxatime,   ed.statuso.st_atime);
    EXPECT_EQ(summary.mincrtime,  ed.crtime);
    EXPECT_EQ(summary.maxcrtime,  ed.crtime);
    EXPECT_EQ(summary.minossint1, ed.ossint1);
    EXPECT_EQ(summary.maxossint1, ed.ossint1);
    EXPECT_EQ(summary.minossint2, ed.ossint2);
    EXPECT_EQ(summary.maxossint2, ed.ossint2);
    EXPECT_EQ(summary.minossint3, ed.ossint3);
    EXPECT_EQ(summary.maxossint3, ed.ossint3);
    EXPECT_EQ(summary.minossint4, ed.ossint4);
    EXPECT_EQ(summary.maxossint4, ed.ossint4);
    EXPECT_EQ(summary.totxattr,   0);

    free(pwork);
}

TEST(summary, tsumit) {
    struct sum in;
    zeroit(&in);

    struct sum out;
    zeroit(&out);

    // values that get summed
    in.totfiles   = 1; out.totfiles   = 1;
    in.totlinks   = 1; out.totlinks   = 1;
    in.totltk     = 1; out.totltk     = 1;
    in.totmtk     = 1; out.totmtk     = 1;
    in.totltm     = 1; out.totltm     = 1;
    in.totmtm     = 1; out.totmtm     = 1;
    in.totmtg     = 1; out.totmtg     = 1;
    in.totmtt     = 1; out.totmtt     = 1;
    in.totsize    = 1; out.totsize    = 1;
    in.totxattr   = 1; out.totxattr   = 1;
    in.totossint1 = 1; out.totossint1 = 1;
    in.totossint2 = 1; out.totossint2 = 1;
    in.totossint3 = 1; out.totossint3 = 1;
    in.totossint4 = 1; out.totossint4 = 1;

    // all of out gets replaced
    out.maxsubdirfiles = 0;
    out.maxsubdirlinks = 0;
    out.maxsubdirsize  = 0;

    in.minuid     = 0; out.minuid     = 1;
    in.maxuid     = 1; out.maxuid     = 0;
    in.mingid     = 0; out.mingid     = 1;
    in.maxgid     = 1; out.maxgid     = 0;
    in.minsize    = 0; out.minsize    = 1;
    in.maxsize    = 1; out.maxsize    = 0;
    in.minblocks  = 0; out.minblocks  = 1;
    in.maxblocks  = 1; out.maxblocks  = 0;
    in.minctime   = 0; out.minctime   = 1;
    in.maxctime   = 1; out.maxctime   = 0;
    in.minmtime   = 0; out.minmtime   = 1;
    in.maxmtime   = 1; out.maxmtime   = 0;
    in.minatime   = 0; out.minatime   = 1;
    in.maxatime   = 1; out.maxatime   = 0;
    in.mincrtime  = 0; out.mincrtime  = 1;
    in.maxcrtime  = 1; out.maxcrtime  = 0;
    in.minossint1 = 0; out.minossint1 = 1;
    in.maxossint1 = 1; out.maxossint1 = 0;
    in.minossint2 = 0; out.minossint2 = 1;
    in.maxossint2 = 1; out.maxossint2 = 0;
    in.minossint3 = 0; out.minossint3 = 1;
    in.maxossint3 = 1; out.maxossint3 = 0;
    in.minossint4 = 0; out.minossint4 = 1;
    in.maxossint4 = 1; out.maxossint4 = 0;

    ASSERT_EQ(tsumit(&in, &out), 0);

    EXPECT_EQ(out.maxsubdirfiles, 1);
    EXPECT_EQ(out.maxsubdirlinks, 1);
    EXPECT_EQ(out.maxsubdirsize,  1);

    EXPECT_EQ(out.totfiles,   2);
    EXPECT_EQ(out.totlinks,   2);
    EXPECT_EQ(out.totltk,     2);
    EXPECT_EQ(out.totmtk,     2);
    EXPECT_EQ(out.totltm,     2);
    EXPECT_EQ(out.totmtk,     2);
    EXPECT_EQ(out.totltm,     2);
    EXPECT_EQ(out.totmtm,     2);
    EXPECT_EQ(out.totmtg,     2);
    EXPECT_EQ(out.totmtt,     2);
    EXPECT_EQ(out.totsize,    2);
    EXPECT_EQ(out.totxattr,   2);
    EXPECT_EQ(out.totossint1, 2);
    EXPECT_EQ(out.totossint2, 2);
    EXPECT_EQ(out.totossint3, 2);
    EXPECT_EQ(out.totossint4, 2);

    EXPECT_EQ(in.minuid,     out.minuid);
    EXPECT_EQ(in.maxuid,     out.maxuid);
    EXPECT_EQ(in.mingid,     out.mingid);
    EXPECT_EQ(in.maxgid,     out.maxgid);
    EXPECT_EQ(in.minsize,    out.minsize);
    EXPECT_EQ(in.maxsize,    out.maxsize);
    EXPECT_EQ(in.minblocks,  out.minblocks);
    EXPECT_EQ(in.maxblocks,  out.maxblocks);
    EXPECT_EQ(in.minctime,   out.minctime);
    EXPECT_EQ(in.maxctime,   out.maxctime);
    EXPECT_EQ(in.minmtime,   out.minmtime);
    EXPECT_EQ(in.maxmtime,   out.maxmtime);
    EXPECT_EQ(in.minatime,   out.minatime);
    EXPECT_EQ(in.maxatime,   out.maxatime);
    EXPECT_EQ(in.mincrtime,  out.mincrtime);
    EXPECT_EQ(in.maxcrtime,  out.maxcrtime);
    EXPECT_EQ(in.minossint1, out.minossint1);
    EXPECT_EQ(in.maxossint1, out.maxossint1);
    EXPECT_EQ(in.minossint2, out.minossint2);
    EXPECT_EQ(in.maxossint2, out.maxossint2);
    EXPECT_EQ(in.minossint3, out.minossint3);
    EXPECT_EQ(in.maxossint3, out.maxossint3);
    EXPECT_EQ(in.minossint4, out.minossint4);
    EXPECT_EQ(in.maxossint4, out.maxossint4);
}

TEST(shortpath, split) {
    #define PATH "a/b/c/d"
    #define LAST "e"
    #define SRC PATH "/" LAST

    char dst[MAXPATH];
    char endname[MAXPATH];

    // path ending with basename
    EXPECT_EQ(shortpath(SRC, dst, endname), 0);
    EXPECT_STREQ(dst, PATH);
    EXPECT_STREQ(endname, LAST);

    // path ending with slash
    EXPECT_EQ(shortpath(SRC "/", dst, endname), 0);
    EXPECT_STREQ(dst, SRC);
    EXPECT_STREQ(endname, "");

    // no slash
    EXPECT_EQ(shortpath(LAST, dst, endname), 0);
    EXPECT_STREQ(dst, "");
    EXPECT_STREQ(endname, LAST);

    // empty string
    EXPECT_EQ(shortpath("", dst, endname), 0);
    EXPECT_STREQ(dst, "");
    EXPECT_STREQ(endname, "");
}

TEST(dupdir, parentfirst) {
    char root[] = "XXXXXX";
    ASSERT_NE(mkdtemp(root), nullptr);

    char parent[MAXPATH];
    EXPECT_GT(SNPRINTF(parent, sizeof(parent), "%s/parent", root), 0);
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[MAXPATH];
    EXPECT_GT(SNPRINTF(child, sizeof(child), "%s/child", parent), 0);
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    ASSERT_EQ(dupdir(parent, &orig_parent_stat), 0);
    ASSERT_EQ(dupdir(child,  &orig_child_stat),  0);

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);
    EXPECT_EQ(rmdir(root),   0);

    EXPECT_EQ(orig_parent_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_parent_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_parent_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR,  child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,             child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,             child_stat.st_gid);
}

TEST(dupdir, childtfirst) {
    char root[] = "XXXXXX";
    ASSERT_NE(mkdtemp(root), nullptr);

    char parent[MAXPATH];
    EXPECT_GT(SNPRINTF(parent, sizeof(parent), "%s/parent", root), 0);
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[MAXPATH];
    EXPECT_GT(SNPRINTF(child, sizeof(child), "%s/child", parent), 0);
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    const mode_t prev_umask = umask(0);

    ASSERT_EQ(dupdir(child,  &orig_child_stat),  0);

    // parent path should have the same permissions as the child path
    {
        struct stat child_stat;
        struct stat parent_stat;
        ASSERT_EQ(stat(child,  &child_stat),  0);
        ASSERT_EQ(stat(parent, &parent_stat), 0);

        EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, child_stat.st_mode);
        EXPECT_EQ(orig_child_stat.st_uid,            child_stat.st_uid);
        EXPECT_EQ(orig_child_stat.st_gid,            child_stat.st_gid);

        EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, parent_stat.st_mode);
        EXPECT_EQ(orig_child_stat.st_uid,            parent_stat.st_uid);
        EXPECT_EQ(orig_child_stat.st_gid,            parent_stat.st_gid);

    }

    ASSERT_EQ(dupdir(parent, &orig_parent_stat), 0);

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    umask(prev_umask);
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);
    EXPECT_EQ(rmdir(root),   0);

    EXPECT_EQ(orig_parent_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_parent_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_parent_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR,  child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,             child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,             child_stat.st_gid);
}

TEST(dupdir, fileasdir) {
    char parent[] = "XXXXXX";
    const int fd = mkstemp(parent);
    ASSERT_GE(fd, -1);
    EXPECT_EQ(close(fd), 0);

    char child[MAXPATH];
    EXPECT_GT(SNPRINTF(child, sizeof(child), "%s/child", parent), 0);
    struct stat child_stat;
    child_stat.st_mode = S_IRWXU; // 700
    child_stat.st_uid = geteuid();
    child_stat.st_gid = getegid();

    EXPECT_NE(dupdir(parent, &child_stat), 0);
    EXPECT_NE(dupdir(child,  &child_stat), 0);
    EXPECT_EQ(remove(parent), 0);
}

TEST(mkpath, parentfirst) {
    char root[] = "XXXXXX";
    ASSERT_NE(mkdtemp(root), nullptr);

    char parent[MAXPATH];
    EXPECT_GT(SNPRINTF(parent, sizeof(parent), "%s/parent", root), 0);
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[MAXPATH];
    EXPECT_GT(SNPRINTF(child, sizeof(child), "%s/child", parent), 0);
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    const mode_t prev_umask = umask(0);
    ASSERT_EQ(mkdir (parent, orig_parent_stat.st_mode), 0);
    #ifdef __APPLE__
    ASSERT_EQ(chown (parent, orig_parent_stat.st_uid, orig_parent_stat.st_gid), 0);
    #endif
    ASSERT_EQ(mkpath(child,  orig_child_stat.st_mode, orig_child_stat.st_uid, orig_child_stat.st_gid),  0);

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    umask(prev_umask);
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);
    EXPECT_EQ(rmdir(root),   0);

    EXPECT_EQ(orig_parent_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_parent_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_parent_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR,  child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,             child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,             child_stat.st_gid);
}

TEST(mkpath, childfirst) {
    char root[] = "XXXXXX";
    ASSERT_NE(mkdtemp(root), nullptr);

    char parent[MAXPATH];
    EXPECT_GT(SNPRINTF(parent, sizeof(parent), "%s/parent", root), 0);
    struct stat orig_parent_stat;
    orig_parent_stat.st_mode = S_IRWXU; // 700
    orig_parent_stat.st_uid = geteuid();
    orig_parent_stat.st_gid = getegid();

    char child[MAXPATH];
    EXPECT_GT(SNPRINTF(child, sizeof(child), "%s/child", parent), 0);
    struct stat orig_child_stat;
    orig_child_stat.st_mode = S_IRWXU | S_IRWXG | S_IRWXO; // 777
    orig_child_stat.st_uid = geteuid();
    orig_child_stat.st_gid = getegid();

    // remove leftover directories
    rmdir(child);
    rmdir(parent);

    const mode_t prev_umask = umask(0);
    ASSERT_EQ(mkpath(child,  orig_child_stat.st_mode, orig_child_stat.st_uid, orig_child_stat.st_gid),   0);
    ASSERT_EQ(mkpath(parent, orig_parent_stat.st_mode, orig_parent_stat.st_uid, orig_parent_stat.st_gid),  -1); // EEXISTS

    struct stat parent_stat;
    struct stat child_stat;
    ASSERT_EQ(stat(parent, &parent_stat), 0);
    ASSERT_EQ(stat(child,  &child_stat),  0);

    // clean up
    umask(prev_umask);
    EXPECT_EQ(rmdir(child),  0);
    EXPECT_EQ(rmdir(parent), 0);
    EXPECT_EQ(rmdir(root),   0);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, parent_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,            parent_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,            parent_stat.st_gid);

    EXPECT_EQ(orig_child_stat.st_mode | S_IFDIR, child_stat.st_mode);
    EXPECT_EQ(orig_child_stat.st_uid,            child_stat.st_uid);
    EXPECT_EQ(orig_child_stat.st_gid,            child_stat.st_gid);
}

TEST(mkpath, fileasdir) {
    char parent[] = "XXXXXX";
    const int fd = mkstemp(parent);
    ASSERT_GE(fd, -1);
    EXPECT_EQ(close(fd), 0);

    // need 2 levels due to how mkpath is implemented
    char child[MAXPATH];
    EXPECT_GT(SNPRINTF(child, sizeof(child), "%s/dir/subdir", parent), 0);
    EXPECT_EQ(mkpath(child, S_IRWXU, geteuid(), getegid()), -1);

    EXPECT_EQ(remove(parent), 0);
}

static char *another_modetostr(char *str, const mode_t mode) {
    static const char rwx[] = "rwx";
    snprintf(str, 11, "----------");
    switch(mode & S_IFMT) {
        case S_IFDIR:
            str[0] = 'd';
            break;
        case S_IFLNK:
            str[0] = 'l';
            break;
    }
    str++;

    for(size_t i = 0; i < 9; i++) {
        if (mode & (1U << (8 - i))) {
            str[i] = rwx[i % 3];
        }
    }

    return --str;
}

TEST(modetostr, error) {
    // no output buffer
    EXPECT_EQ(modetostr(nullptr, 1000, 0), nullptr);

    // buffer is too small
    for(size_t i = 0; i < 11; i++) {
        char addr = 0;
        EXPECT_EQ(modetostr(&addr, i, 0), nullptr);
    }
}

TEST(modetostr, files) {
    char actual[11];
    char expected[11];
    for(mode_t i = 0; i < 01000; i++) {
        EXPECT_STREQ(modetostr(actual, 11, i), another_modetostr(expected, i));
    }
}

TEST(modetostr, links) {
    char actual[11];
    char expected[11];
    for(mode_t i = 0; i < 01000; i++) {
        const mode_t mode = i | S_IFLNK;
        EXPECT_STREQ(modetostr(actual, 11, mode), another_modetostr(expected, mode));
    }
}

TEST(modetostr, directories) {
    char actual[11];
    char expected[11];
    for(mode_t i = 0; i < 01000; i++) {
        const mode_t mode = i | S_IFDIR;
        EXPECT_STREQ(modetostr(actual, 11, mode), another_modetostr(expected, mode));
    }
}

TEST(trailing_match_index, paths) {
    const char root[]    = "/";
    const char dir[]     = "/a/b/c/";
    const char nondir[]  = "/a/b/c";
    const char slashes[] = "////";
    const char nulls[]   = "";

    const char match[] = "\b\t/";
    const size_t match_len = strlen(match);

    EXPECT_EQ(trailing_match_index(root,    strlen(root),    match, match_len), (size_t) 1);
    EXPECT_EQ(trailing_match_index(dir,     strlen(dir),     match, match_len), (size_t) 7);
    EXPECT_EQ(trailing_match_index(nondir,  strlen(nondir),  match, match_len), (size_t) 5);
    EXPECT_EQ(trailing_match_index(slashes, strlen(slashes), match, match_len), (size_t) 4);
    EXPECT_EQ(trailing_match_index(nulls,   strlen(nulls),   match, match_len), (size_t) 0);

    EXPECT_EQ(trailing_match_index(nullptr, 10,              match, match_len), (size_t) 0);
}

TEST(trailing_non_match_index, paths) {
    const char root[]    = "/";
    const char dir[]     = "/a/b/c/";
    const char nondir[]  = "/a/b/c";
    const char slashes[] = "////";
    const char nulls[]   = "";

    const char match[] = "\b\t/";
    const size_t match_len = strlen(match);

    EXPECT_EQ(trailing_non_match_index(root,    strlen(root),    match, match_len), (size_t) 0);
    EXPECT_EQ(trailing_non_match_index(dir,     strlen(dir),     match, match_len), (size_t) 6);
    EXPECT_EQ(trailing_non_match_index(nondir,  strlen(nondir),  match, match_len), (size_t) 6);
    EXPECT_EQ(trailing_non_match_index(slashes, strlen(slashes), match, match_len), (size_t) 0);
    EXPECT_EQ(trailing_non_match_index(nulls,   strlen(nulls),   match, match_len), (size_t) 0);

    EXPECT_EQ(trailing_non_match_index(nullptr, 10,              match, match_len), (size_t) 0);
}

TEST(setup_directory_skip, file) {
    const std::string skip_dirs[] = {
        "skip0",
        "skip1",
        "skip0", // duplicate
    };

    char skip_name[] = "XXXXXX";
    const int skip_fd = mkstemp(skip_name);
    ASSERT_NE(skip_fd, -1);
    EXPECT_EQ(close(skip_fd), 0);

    {
        std::ofstream skip_stream(skip_name);
        EXPECT_TRUE(skip_stream);

        skip_stream << std::endl; // start with empty line
        for(std::string const & skip_dir : skip_dirs) {
            skip_stream << skip_dir << std::endl;
        }
    }

    trie_t *skip = trie_alloc();
    ASSERT_NE(skip, nullptr);
    EXPECT_EQ(setup_directory_skip(skip, skip_name), (ssize_t) (sizeof(skip_dirs) / sizeof(skip_dirs[0])) - 1);
    for(std::string const & skip_dir : skip_dirs) {
        EXPECT_EQ(trie_search(skip, skip_dir.c_str(), skip_dir.size(), nullptr), 1);
    }
    trie_free(skip);

    EXPECT_EQ(remove(skip_name), 0);
}

TEST(setup_directory_skip, bad) {
    trie_t *skip = trie_alloc();
    ASSERT_NE(skip, nullptr);

    char name[] = "XXXXXX";
    const int fd = mkstemp(name);
    ASSERT_GT(fd, -1);
    EXPECT_EQ(setup_directory_skip(skip, name), 0);
    EXPECT_EQ(close(fd), 0);
    EXPECT_EQ(remove(name), 0);

    EXPECT_EQ(setup_directory_skip(skip, ""), -1);
    EXPECT_EQ(setup_directory_skip(skip, nullptr), -1);

    trie_free(skip);

    EXPECT_EQ(setup_directory_skip(nullptr, ""), -1);
}

TEST(split, delims) {
    const char delims[] = "\xfe\xff";
    char line[MAXPATH];
    const size_t line_len = SNFORMAT_S(line, MAXPATH, 6,
                                       "a", 1,
                                       delims, 1,
                                       "b", 1,
                                       &delims[1], 1,
                                       delims, 1,
                                       "c", 1);
    EXPECT_EQ(line_len, (size_t) 6);
    const char *end = line + line_len;

    char *curr = nullptr;
    char *next = line;

    curr = next; next = split(curr, delims, 2, end);
    EXPECT_STREQ(curr, "a");

    curr = next; next = split(curr, delims, 2, end);
    EXPECT_STREQ(curr, "b");

    curr = next; next = split(curr, delims, 2, end);
    EXPECT_STREQ(curr, "");

    curr = next; next = split(curr, delims, 2, end);
    EXPECT_STREQ(curr, "c");

    curr = next; next = split(curr, delims, 2, end);
    EXPECT_EQ(curr, nullptr);
    EXPECT_EQ(next, nullptr);

    /* input checks */
    next = split(nullptr, delims, 2, end);
    EXPECT_EQ(next, nullptr);

    next = split(line, nullptr, 2, end);
    EXPECT_EQ(next, nullptr);

    next = split(line, delims, 0, end);
    EXPECT_EQ(next, nullptr);

    next = split(line, delims, 2, nullptr);
    EXPECT_EQ(next, nullptr);

    next = split((char *) end + 1, delims, 2, end);
    EXPECT_EQ(next, nullptr);
}

TEST(getline, fd) {
    const size_t default_size = 1024;
    const size_t LINE_LEN = 1025;
    char LINE[LINE_LEN] = "";

    memset(LINE, 'X', LINE_LEN);

    char name[] = "XXXXXX";
    const int fd = mkstemp(name);
    ASSERT_GT(fd, -1);
    EXPECT_EQ(remove(name), 0);

    // line 1
    EXPECT_EQ(write(fd, LINE, LINE_LEN), (ssize_t) LINE_LEN);
    EXPECT_EQ(write(fd, "\n", 1),        (ssize_t) 1);

    // line 2
    EXPECT_EQ(write(fd, "\n", 1),        (ssize_t) 1);

    char *line = nullptr;
    size_t size = 0; // size of buffer, not length of string
    off_t offset = 0;

    // read line
    EXPECT_EQ(getline_fd(&line, &size, fd, &offset, default_size), (ssize_t) LINE_LEN);
    EXPECT_NE(line,   nullptr);
    EXPECT_NE(size,   (size_t) 0);
    EXPECT_EQ(offset, (off_t) (LINE_LEN + 1));
    free(line);
    line = nullptr;

    // empty line
    EXPECT_EQ(getline_fd(&line, &size, fd, &offset, default_size), (ssize_t) 0);
    EXPECT_NE(line,   nullptr);
    EXPECT_NE(size,   (size_t) 0);
    EXPECT_EQ(offset, (off_t) (LINE_LEN + 2));
    free(line);
    line = nullptr;

    // end of file
    EXPECT_EQ(getline_fd(&line, &size, fd, &offset, default_size), (ssize_t) -EIO);
    EXPECT_NE(line,   nullptr);
    EXPECT_NE(size,   (size_t) 0);
    EXPECT_EQ(offset, (off_t) (LINE_LEN + 2));
    free(line);
    line = nullptr;

    /* past end of file */
    const off_t past = LINE_LEN * 5;
    offset = past;
    EXPECT_EQ(getline_fd(&line, &size, fd, &offset, default_size), (ssize_t) -EIO);
    EXPECT_NE(line,   nullptr);
    EXPECT_NE(size,   (size_t) 0);
    EXPECT_EQ(offset, past);
    free(line);
    line = nullptr;

    EXPECT_EQ(close(fd), 0);

    /* closed valid file */
    offset = 0;
    EXPECT_LT(getline_fd(&line, &size, fd, &offset, default_size), (ssize_t) 0);
    EXPECT_NE(line,   nullptr);
    EXPECT_NE(size,   (size_t) 0);
    EXPECT_EQ(offset, (off_t) 0);
    free(line);
    line = nullptr;

    /* read from stdout */
    EXPECT_LT(getline_fd(&line, &size, STDOUT_FILENO, &offset, default_size), (ssize_t) 0);
    EXPECT_NE(line,   nullptr);
    EXPECT_NE(size,   (size_t) 0);
    EXPECT_EQ(offset, (off_t) 0);
    free(line);
    line = nullptr;

    /* bad inputs */
    EXPECT_EQ(getline_fd(nullptr, &size,   fd, &offset, default_size), -EINVAL);
    EXPECT_EQ(getline_fd(&line,   nullptr, fd, &offset, default_size), -EINVAL);
    EXPECT_EQ(getline_fd(&line,   &size,   -1, &offset, default_size), -EINVAL);
    EXPECT_EQ(getline_fd(&line,   &size,   fd, nullptr, default_size), -EINVAL);
    EXPECT_EQ(getline_fd(&line,   &size,   fd, &offset, 0),            -EINVAL);

    /* initial alloc is too big */
    size = (size_t) -1;
    EXPECT_EQ(getline_fd(&line,   &size,   fd, &offset, default_size), -ENOMEM);
    EXPECT_EQ(line, nullptr);
}

TEST(copyfd, good) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution <> dist(0, 255);

    char srcname[] = "XXXXXX";
    int src = mkstemp(srcname);
    ASSERT_GT(src, -1);

    const std::size_t size = 4096;

    char srcbuf[size];
    for(std::size_t i = 0; i < size; i++) {
        srcbuf[i] = dist(gen);
    }

    size_t remaining = size;
    while (remaining) {
        const ssize_t w = write(src, srcbuf + size - remaining, remaining);
        ASSERT_GT(w, (ssize_t) 0);
        remaining -= w;
    }

    struct stat srcst;
    ASSERT_EQ(fstat(src, &srcst), 0);
    ASSERT_EQ(srcst.st_size, (off_t) size);

    const off_t offsets[] = {(off_t) 0,
                             (off_t) ((dist(gen) * dist(gen)) % size),
                             (off_t) ((dist(gen) * dist(gen)) % size)};

    for(std::size_t i = 0; i < sizeof(offsets) / sizeof(offsets[0]); i++) {
        // new dst each time
        char dstname[] = "XXXXXX";
        int dst = mkstemp(dstname);
        ASSERT_GT(dst, -1);

        const ssize_t copy_size = size - offsets[i];

        ASSERT_EQ(lseek(src, 0, SEEK_SET), 0);
        // dst is new, so the position is still at 0

        EXPECT_EQ(copyfd(src, offsets[i], dst, 0, size), copy_size);

        // dst will have been moved
        EXPECT_EQ(lseek(dst, 0, SEEK_SET), 0);

        struct stat dstst;
        EXPECT_EQ(fstat(dst, &dstst), 0);
        EXPECT_EQ(dstst.st_size, (off_t) copy_size);

        char dstbuf[size];

        remaining = copy_size;
        while (remaining) {
            const ssize_t r = read(dst, dstbuf + copy_size - remaining, remaining);
            ASSERT_GT(r, (ssize_t) 0);
            remaining -= r;
        }
        EXPECT_EQ(memcmp(dstbuf, srcbuf + offsets[i], copy_size), 0);

        EXPECT_EQ(close(dst), 0);
        EXPECT_EQ(remove(dstname), 0);
    }

    EXPECT_EQ(close(src), 0);
    EXPECT_EQ(remove(srcname), 0);
}

TEST(copyfd, bad) {
    char srcname[] = "XXXXXX";
    int src = mkstemp(srcname);
    ASSERT_GT(src, -1);
    ASSERT_EQ(pwrite(src, "bad", 3, 0), 3);

    char dstname[] = "XXXXXX";
    int dst = mkstemp(dstname);
    ASSERT_GT(dst, -1);

    EXPECT_EQ(copyfd(-1,  0, dst, 0, 1), (ssize_t) -1);
    EXPECT_EQ(copyfd(src, 0, -1,  0, 1), (ssize_t) -1);

    EXPECT_EQ(close(dst), 0);
    EXPECT_EQ(remove(dstname), 0);

    EXPECT_EQ(close(src), 0);
    EXPECT_EQ(remove(srcname), 0);
}

TEST(set_metadata, bad) {
    char dir[] = "XXXXXX";
    ASSERT_NE(mkdtemp(dir), nullptr);

    char path[MAXPATH];
    snprintf(path, sizeof(path), "%s/file", dir);

    struct stat st;
    memset(&st, 0, sizeof(st));

    struct xattrs xattrs;
    xattrs_setup(&xattrs);
    xattrs.count = 1;
    xattrs_alloc(&xattrs);
    xattrs.pairs[0].name_len  = snprintf(xattrs.pairs[0].name,  sizeof(xattrs.pairs[0].name),  "xattr_name");
    xattrs.pairs[0].value_len = snprintf(xattrs.pairs[0].value, sizeof(xattrs.pairs[0].value), "xattr_value");

    EXPECT_NO_THROW(set_metadata(path, &st, &xattrs));

    xattrs_cleanup(&xattrs);

    EXPECT_EQ(rmdir(dir), 0);
}

TEST(write_read, size) {
    char filename[] = "XXXXXX";
    int fd = mkstemp(filename);
    ASSERT_GT(fd, -1);
    EXPECT_EQ(remove(filename), 0);

    const off_t start = lseek(fd, 0, SEEK_CUR);
    EXPECT_EQ(start, 0);

    char src[1024];
    EXPECT_EQ(memset(src, 0xa5, sizeof(src)), src);
    EXPECT_EQ(write_size(fd, src, sizeof(src)), (ssize_t) sizeof(src));

    const off_t end = lseek(fd, 0, SEEK_CUR);
    EXPECT_EQ(end, (off_t) sizeof(src));

    EXPECT_EQ(lseek(fd, 0, SEEK_SET), 0);

    char dst[1024];
    EXPECT_EQ(memset(dst, 0, sizeof(dst)), dst);
    EXPECT_EQ(read_size(fd, dst, sizeof(dst)), (ssize_t) sizeof(dst));

    EXPECT_EQ(memcmp(dst, src, sizeof(src)), 0);

    EXPECT_EQ(close(fd), 0);

    // write to closed file descriptor
    EXPECT_EQ(read_size(fd, dst, sizeof(dst)), -1);
}
