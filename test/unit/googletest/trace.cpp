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
static const size_t EXPECTED_XATTRS_STR_LEN = sizeof(EXPECTED_XATTRS_STR) - 1;

static void get_work(struct work *work, struct entry_data *ed) {
    work->name_len         = snprintf(work->name, sizeof(work->name), "name");
    ed->type               = 't';
    snprintf(ed->linkname, sizeof(ed->linkname), "link");
    ed->statuso.st_ino     = 0xfedc;
    ed->statuso.st_mode    = 0777;
    ed->statuso.st_nlink   = 2048;
    ed->statuso.st_uid     = 1001;
    ed->statuso.st_gid     = 1001;
    ed->statuso.st_size    = 4096;
    ed->statuso.st_blksize = 4096;
    ed->statuso.st_blocks  = 1;
    ed->statuso.st_atime   = 0x1234;
    ed->statuso.st_mtime   = 0x5678;
    ed->statuso.st_ctime   = 0x9abc;
    ed->xattrs.pairs       = (struct xattr *) EXPECTED_XATTR;
    ed->xattrs.name_len    = 0;
    ed->xattrs.len         = 0;
    ed->xattrs.count       = 2;
    ed->crtime             = 0x9abc;
    ed->ossint1            = 1;
    ed->ossint2            = 2;
    ed->ossint3            = 3;
    ed->ossint4            = 4;
    snprintf(ed->osstext1, sizeof(ed->osstext1), "osstext1");
    snprintf(ed->osstext2, sizeof(ed->osstext2), "osstext2");
    work->pinode           = 0xdef0;
}

static int to_string(char *line, const size_t size, struct work *work, struct entry_data *ed) {
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
                               work->name,             delim,
                               ed->type,               delim,
                               ed->statuso.st_ino,     delim,
                               ed->statuso.st_mode,    delim,
                               ed->statuso.st_nlink,   delim,
                               ed->statuso.st_uid,     delim,
                               ed->statuso.st_gid,     delim,
                               ed->statuso.st_size,    delim,
                               ed->statuso.st_blksize, delim,
                               ed->statuso.st_blocks,  delim,
                               ed->statuso.st_atime,   delim,
                               ed->statuso.st_mtime,   delim,
                               ed->statuso.st_ctime,   delim,
                               ed->linkname,           delim);

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
                                             delim,
                               ed->crtime,   delim,
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
    struct work work;
    struct entry_data ed;
    get_work(&work, &ed);

    // write a known struct to a file
    char buf[4096];
    FILE *file = fmemopen(buf, sizeof(buf), "w");
    ASSERT_NE(file, nullptr);

    const int written = worktofile(file, delim, 0, &work, &ed);
    fclose(file);

    ASSERT_GT(written, 0);
    ASSERT_LT(written, (int) sizeof(buf));

    // generate the string to compare with
    char line[4096];
    const int rc = to_string(line, sizeof(line), &work, &ed);

    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    EXPECT_STREQ(buf, line);

    EXPECT_EQ(worktofile(NULL, delim, 0, &work, &ed),  -1);
    EXPECT_EQ(worktofile(file, delim, 0, NULL,  &ed),  -1);
    EXPECT_EQ(worktofile(file, delim, 0, &work, NULL), -1);
}

#define COMPARE(src, src_ed, dst, dst_ed)                               \
    EXPECT_STREQ(dst.name,               src.name);                     \
    EXPECT_EQ(dst_ed.type,               src_ed.type);                  \
    EXPECT_EQ(dst_ed.statuso.st_ino,     src_ed.statuso.st_ino);        \
    EXPECT_EQ(dst_ed.statuso.st_mode,    src_ed.statuso.st_mode);       \
    EXPECT_EQ(dst_ed.statuso.st_nlink,   src_ed.statuso.st_nlink);      \
    EXPECT_EQ(dst_ed.statuso.st_uid,     src_ed.statuso.st_uid);        \
    EXPECT_EQ(dst_ed.statuso.st_gid,     src_ed.statuso.st_gid);        \
    EXPECT_EQ(dst_ed.statuso.st_size,    src_ed.statuso.st_size);       \
    EXPECT_EQ(dst_ed.statuso.st_blksize, src_ed.statuso.st_blksize);    \
    EXPECT_EQ(dst_ed.statuso.st_blocks,  src_ed.statuso.st_blocks);     \
    EXPECT_EQ(dst_ed.statuso.st_atime,   src_ed.statuso.st_atime);      \
    EXPECT_EQ(dst_ed.statuso.st_mtime,   src_ed.statuso.st_mtime);      \
    EXPECT_EQ(dst_ed.statuso.st_ctime,   src_ed.statuso.st_ctime);      \
    EXPECT_STREQ(dst_ed.linkname,        src_ed.linkname);              \
    EXPECT_EQ(dst_ed.crtime,             src_ed.crtime);                \
    EXPECT_EQ(dst_ed.ossint1,            src_ed.ossint1);               \
    EXPECT_EQ(dst_ed.ossint2,            src_ed.ossint2);               \
    EXPECT_EQ(dst_ed.ossint3,            src_ed.ossint3);               \
    EXPECT_EQ(dst_ed.ossint4,            src_ed.ossint4);               \
    EXPECT_STREQ(dst_ed.osstext1,        src_ed.osstext1);              \
    EXPECT_STREQ(dst_ed.osstext2,        src_ed.osstext2);              \
    EXPECT_EQ(dst.pinode,                src.pinode);                   \

TEST(trace, linetowork) {
    struct work src;
    struct entry_data src_ed;
    get_work(&src, &src_ed);

    // write the known struct to a string using an alternative write function
    char line[4096];
    const int rc = to_string(line, sizeof(line), &src, &src_ed);
    ASSERT_GT(rc, -1);
    ASSERT_LT(rc, (int) sizeof(line));

    // read the string
    struct work work;
    struct entry_data ed;
    refstr_t attachname;
    EXPECT_EQ(linetowork(line, rc, delim, &work, &ed, &attachname), 0);

    COMPARE(src, src_ed, work, ed);

    EXPECT_STREQ(ed.xattrs.pairs[0].name,  EXPECTED_XATTRS.pairs[0].name);
    EXPECT_STREQ(ed.xattrs.pairs[0].value, EXPECTED_XATTRS.pairs[0].value);
    EXPECT_STREQ(ed.xattrs.pairs[1].name,  EXPECTED_XATTRS.pairs[1].name);
    EXPECT_STREQ(ed.xattrs.pairs[1].value, EXPECTED_XATTRS.pairs[1].value);

    xattrs_cleanup(&ed.xattrs);

    EXPECT_EQ(linetowork(NULL, rc, delim, &work, &ed,  &attachname), -1);
    EXPECT_EQ(linetowork(line, rc, delim, NULL,  &ed,  &attachname), -1);
    EXPECT_EQ(linetowork(line, rc, delim, &work, NULL, &attachname), -1);

    src_ed.type = 'e';
    EXPECT_EQ(to_string(line, sizeof(line), &src, &src_ed), rc);
    EXPECT_EQ(linetowork(line, rc, delim, &work, &ed, NULL), -1);
}
