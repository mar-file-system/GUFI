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

extern "C" {
#include "config.h"
#include "trace.h"
}

static const char delim[] = "\x1e";

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
static const size_t EXPECTED_XATTRS_STR_LEN = sizeof(EXPECTED_XATTRS_STR) - 1;

static struct work *get_work() {
    struct work *work = new struct work;
    work->name_len           = snprintf(work->name, sizeof(work->name), "name");
    snprintf(work->type,     sizeof(work->type), "t");
    snprintf(work->linkname, sizeof(work->linkname), "link");
    work->statuso.st_ino     = 0xfedc;
    work->statuso.st_mode    = 0777;
    work->statuso.st_nlink   = 2048;
    work->statuso.st_uid     = 1001;
    work->statuso.st_gid     = 1001;
    work->statuso.st_size    = 4096;
    work->statuso.st_blksize = 4096;
    work->statuso.st_blocks  = 1;
    work->statuso.st_atime   = 0x1234;
    work->statuso.st_mtime   = 0x5678;
    work->statuso.st_ctime   = 0x9abc;
    work->xattrs.pairs       = (struct xattr *) EXPECTED_XATTR;
    work->xattrs.name_len    = 0;
    work->xattrs.len         = 0;
    work->xattrs.count       = 2;
    work->crtime             = 0x9abc;
    work->ossint1            = 1;
    work->ossint2            = 2;
    work->ossint3            = 3;
    work->ossint4            = 4;
    snprintf(work->osstext1, sizeof(work->osstext1), "osstext1");
    snprintf(work->osstext2, sizeof(work->osstext2), "osstext2");
    work->pinode             = 0xdef0;
    return work;
}

static int to_string(char *line, const size_t size, struct work *work) {
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
                               work->name,               delim[0],
                               work->type[0],            delim[0],
                               work->statuso.st_ino,     delim[0],
                               work->statuso.st_mode,    delim[0],
                               work->statuso.st_nlink,   delim[0],
                               work->statuso.st_uid,     delim[0],
                               work->statuso.st_gid,     delim[0],
                               work->statuso.st_size,    delim[0],
                               work->statuso.st_blksize, delim[0],
                               work->statuso.st_blocks,  delim[0],
                               work->statuso.st_atime,   delim[0],
                               work->statuso.st_mtime,   delim[0],
                               work->statuso.st_ctime,   delim[0],
                               work->linkname,           delim[0]);

    line += part1;

    memcpy(line, EXPECTED_XATTRS_STR, EXPECTED_XATTRS_STR_LEN);
    line += EXPECTED_XATTRS_STR_LEN;

    const int part2 = snprintf(line, size,
                               "%c"
                               "%d%c"
                               "%d%c"
                               "%d%c"
                               "%d%c"
                               "%d%c"
                               "%s%c"
                               "%s%c"
                               "%lld%c"
                               "\n",
                                               delim[0],
                               work->crtime,   delim[0],
                               work->ossint1,  delim[0],
                               work->ossint2,  delim[0],
                               work->ossint3,  delim[0],
                               work->ossint4,  delim[0],
                               work->osstext1, delim[0],
                               work->osstext2, delim[0],
                               work->pinode,   delim[0]);

    line += part2;
    *line = '\0';

    return part1 + EXPECTED_XATTRS_STR_LEN + part2;
}

TEST(trace, worktofile) {
    struct work *work = get_work();
    ASSERT_NE(work, nullptr);

    // write a known struct to a file
    char buf[4096];
    FILE *file = fmemopen(buf, sizeof(buf), "w");
    ASSERT_NE(file, nullptr);

    const int written = worktofile(file, delim, 0, work);
    fclose(file);

    ASSERT_GT(written, 0);
    ASSERT_LT(written, (int) sizeof(buf));

    // generate the string to compare with
    char line[4096];
    const int rc = to_string(line, sizeof(line), work);
    delete work;

    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    EXPECT_STREQ(buf, line);
}

#define COMPARE(src, dst)                                          \
    EXPECT_STREQ(dst.name,            src->name);                  \
    EXPECT_STREQ(dst.type,            src->type);                  \
    EXPECT_EQ(dst.statuso.st_ino,     src->statuso.st_ino);        \
    EXPECT_EQ(dst.statuso.st_mode,    src->statuso.st_mode);       \
    EXPECT_EQ(dst.statuso.st_nlink,   src->statuso.st_nlink);      \
    EXPECT_EQ(dst.statuso.st_uid,     src->statuso.st_uid);        \
    EXPECT_EQ(dst.statuso.st_gid,     src->statuso.st_gid);        \
    EXPECT_EQ(dst.statuso.st_size,    src->statuso.st_size);       \
    EXPECT_EQ(dst.statuso.st_blksize, src->statuso.st_blksize);    \
    EXPECT_EQ(dst.statuso.st_blocks,  src->statuso.st_blocks);     \
    EXPECT_EQ(dst.statuso.st_atime,   src->statuso.st_atime);      \
    EXPECT_EQ(dst.statuso.st_mtime,   src->statuso.st_mtime);      \
    EXPECT_EQ(dst.statuso.st_ctime,   src->statuso.st_ctime);      \
    EXPECT_STREQ(dst.linkname,        src->linkname);              \
    EXPECT_EQ(dst.crtime,             src->crtime);                \
    EXPECT_EQ(dst.ossint1,            src->ossint1);               \
    EXPECT_EQ(dst.ossint2,            src->ossint2);               \
    EXPECT_EQ(dst.ossint3,            src->ossint3);               \
    EXPECT_EQ(dst.ossint4,            src->ossint4);               \
    EXPECT_STREQ(dst.osstext1,        src->osstext1);              \
    EXPECT_STREQ(dst.osstext2,        src->osstext2);              \
    EXPECT_EQ(dst.pinode,             src->pinode);                \

TEST(trace, linetowork) {
    struct work *src = get_work();

    // write the known struct to a string using an alternative write function
    char line[4096];
    const int rc = to_string(line, sizeof(line), src);
    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    // read the string
    struct work work;
    EXPECT_EQ(linetowork(line, rc, delim, &work), 0);

    COMPARE(src, work);

    EXPECT_STREQ(work.xattrs.pairs[0].name,  EXPECTED_XATTRS.pairs[0].name);
    EXPECT_STREQ(work.xattrs.pairs[0].value, EXPECTED_XATTRS.pairs[0].value);
    EXPECT_STREQ(work.xattrs.pairs[1].name,  EXPECTED_XATTRS.pairs[1].name);
    EXPECT_STREQ(work.xattrs.pairs[1].value, EXPECTED_XATTRS.pairs[1].value);

    xattrs_cleanup(&work.xattrs);

    delete src;
}
