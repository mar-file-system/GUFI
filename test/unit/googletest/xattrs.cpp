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



#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <gtest/gtest.h>

#include "trace.h"
#include "xattrs.h"

static const struct xattr EXPECTED_XATTR[] = {
    {
        "user.key1", 9,
        "value1", 6,
    },
    {
        "user.key2", 9,
        "value2", 6,
    },
};

static const struct xattrs EXPECTED_XATTRS = {
    (struct xattr *) EXPECTED_XATTR, 18, 30, 2,
};

static void check_contents(struct xattrs &xattrs) {
    for(size_t i = 0; i < xattrs.count; i++) {
        struct xattr *expected_xattr = &EXPECTED_XATTRS.pairs[i];
        struct xattr *xattr = &xattrs.pairs[i];

        EXPECT_EQ(xattr->name_len, expected_xattr->name_len);
        EXPECT_EQ(xattr->value_len, expected_xattr->value_len);

        EXPECT_EQ(memcmp(xattr->name,  expected_xattr->name,  expected_xattr->name_len),  0);
        EXPECT_EQ(memcmp(xattr->value, expected_xattr->value, expected_xattr->value_len), 0);
    }
}

TEST(xattrs, alloc) {
    EXPECT_EQ(xattrs_alloc(nullptr), 1);

    struct xattrs xattrs;
    xattrs.pairs = (xattr *) (uintptr_t) 1;
    EXPECT_EQ(xattrs_alloc(&xattrs), 1);
}

TEST(xattrs, setup_cleanup) {
    // Setup clears all bookkeeping, and cleanup safely handles empty storage.
    struct xattrs xattrs = {};
    ASSERT_EQ(xattrs_setup(&xattrs), 0);
    EXPECT_EQ(xattrs.pairs, nullptr);
    EXPECT_EQ(xattrs.count, (std::size_t) 0);
    xattrs_cleanup(&xattrs);
    xattrs_cleanup(&xattrs);
}

TEST(xattrs, can_rollin) {
    // Exercise the public-permission rules and the fallback rejection path.
    struct stat parent = {};
    struct stat entry = {};

    entry.st_mode = 0004;
    EXPECT_EQ(xattr_can_rollin(&parent, &entry), 1);

    parent.st_mode = 0666;
    entry.st_mode = 0;
    EXPECT_EQ(xattr_can_rollin(&parent, &entry), 4);

    parent.st_mode = 0660;
    parent.st_gid = entry.st_gid = 2;
    entry.st_uid = parent.st_uid + 1;
    EXPECT_EQ(xattr_can_rollin(&parent, &entry), 4);

    parent.st_mode = 0600;
    parent.st_uid = entry.st_uid = 1;
    entry.st_gid = parent.st_gid + 1;
    EXPECT_EQ(xattr_can_rollin(&parent, &entry), 4);

    parent.st_mode = 0;
    EXPECT_EQ(xattr_can_rollin(&parent, &entry), 0);
}

TEST(xattrs, remove) {
    // Remove multiple matching pairs, compact the array, and handle no match.
    struct xattr pairs[] = {
        {{'f', 'i', 'r', 's', 't'}, 5, {'o', 'n', 'e'}, 3},
        {{'f', 'i', 'r', 's', 't'}, 5, {'o', 'n', 'e',  'x'}, 4},
        {{'s', 'e', 'c', 'o', 'n', 'd'}, 6, {'t', 'w', 'o'}, 3},
    };
    struct xattrs xattrs = {pairs, 16, 26, 3};

    EXPECT_EQ(xattr_remove(&xattrs, "first", 5), 2U);
    EXPECT_EQ(xattrs.count, 1U);
    EXPECT_EQ(xattrs.name_len, 6U);
    EXPECT_EQ(xattrs.len, 9U);
    EXPECT_STREQ(xattrs.pairs[0].name, "second");

    EXPECT_EQ(xattr_remove(&xattrs, "second", 6), 1U);
    EXPECT_EQ(xattrs.count, 0U);
    EXPECT_EQ(xattrs.name_len, 0U);
    EXPECT_EQ(xattrs.len, 0U);

    EXPECT_EQ(xattr_remove(&xattrs, "missing", 7), 0U);
}

TEST(xattrs, get) {
    char name[] = "XXXXXX";
    const int fd = mkstemp(name);
    ASSERT_NE(fd, -1);
    close(fd);

    for(size_t i = 0; i < EXPECTED_XATTRS.count; i++) {
        struct xattr *xattr = &EXPECTED_XATTRS.pairs[i];
        ASSERT_EQ(SETXATTR(name, xattr->name, xattr->value, xattr->value_len), 0);
    }

    /* use fresh xattrs struct */
    struct xattrs xattrs;
    ASSERT_EQ(xattrs_setup(&xattrs), 0);
    ASSERT_EQ(xattrs_get(name, &xattrs), (ssize_t) EXPECTED_XATTRS.count);
    ASSERT_NE(xattrs.pairs, nullptr);
    EXPECT_EQ(xattrs.name_len, EXPECTED_XATTRS.name_len);
    EXPECT_EQ(xattrs.len, EXPECTED_XATTRS.len);
    EXPECT_EQ(xattrs.count, EXPECTED_XATTRS.count);

    check_contents(xattrs);

    xattrs_cleanup(&xattrs);
    remove(name);
}

TEST(xattrs, get_names) {
    const char EXPECTED_STR[] =
        "user.key1\x1F"
        "user.key2\x1F";
    const std::size_t EXPECTED_STR_LEN = sizeof(EXPECTED_STR) - 1;

    EXPECT_EQ(EXPECTED_XATTRS.name_len + EXPECTED_XATTRS.count, EXPECTED_STR_LEN);

    char names[MAXXATTR];
    EXPECT_EQ(xattr_get_names(&EXPECTED_XATTRS, names, sizeof(names), XATTRDELIM),
              (ssize_t) EXPECTED_STR_LEN);
    EXPECT_EQ(memcmp(names, EXPECTED_STR, EXPECTED_STR_LEN), 0);

    // not enough buffer space
    EXPECT_EQ(xattr_get_names(&EXPECTED_XATTRS, names, 0, XATTRDELIM),
              (ssize_t) -1);

    char exact[sizeof(EXPECTED_STR)];
    // Exact capacity is sufficient for all names and separators.
    EXPECT_EQ(xattr_get_names(&EXPECTED_XATTRS, exact, sizeof(exact), XATTRDELIM),
              (ssize_t) EXPECTED_STR_LEN);
    EXPECT_EQ(memcmp(exact, EXPECTED_STR, EXPECTED_STR_LEN), 0);

    // An empty xattr set requires no output space.
    const struct xattrs empty = {nullptr, 0, 0, 0};
    EXPECT_EQ(xattr_get_names(&empty, names, 0, XATTRDELIM), (ssize_t) 0);
}

TEST(xattrs, to_file) {
    const char EXPECTED_STR[] =
        "0026"
        "0002"
        "user.key1\x1Fvalue1\x1F"
        "user.key2\x1Fvalue2\x1F";
    const std::size_t EXPECTED_STR_LEN = sizeof(EXPECTED_STR) - 1;

    char line[MAXXATTR];
    FILE *file = fmemopen(line, MAXXATTR, "wb");

    EXPECT_EQ(xattrs_to_file(file, &EXPECTED_XATTRS, XATTRDELIM), (int) EXPECTED_STR_LEN);
    fclose(file);

    EXPECT_EQ(memcmp(line, EXPECTED_STR, EXPECTED_STR_LEN), 0);

    char *buffer = nullptr;
    size_t size = 0;
    size_t offset = 0;
    // Buffer serialization grows dynamically and preserves the encoded bytes.
    EXPECT_EQ(xattrs_to_buffer(&buffer, &size, &offset, &EXPECTED_XATTRS, XATTRDELIM), 0);
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(offset, EXPECTED_STR_LEN);
    EXPECT_EQ(memcmp(buffer, EXPECTED_STR, EXPECTED_STR_LEN), 0);
    free(buffer);

    struct xattrs empty = {nullptr, 0, 0, 0};
    char empty_line[32] = {};
    // A zero-pair record still emits both fixed-width headers.
    file = fmemopen(empty_line, sizeof(empty_line), "w");
    ASSERT_NE(file, nullptr);
    EXPECT_EQ(xattrs_to_file(file, &empty, XATTRDELIM), 8);
    EXPECT_EQ(fclose(file), 0);
    EXPECT_STREQ(empty_line, "00040000");
}

TEST(xattrs, from_line) {
    // old format
    {
        char line[] =
            "user.key1\x1Fvalue1\x1F"
            "user.key2\x1Fvalue2\x1F";
        const std::size_t line_len = sizeof(line) - 1;
        EXPECT_EQ(line_len, EXPECTED_XATTRS.len + 2 * EXPECTED_XATTRS.count);

        struct xattrs xattrs;
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 1),
                  line + line_len);

        check_contents(xattrs);

        xattrs_cleanup(&xattrs);
    }

    // new format
    {
        const char orig[] =
            "0026"
            "0002"
            "user.key1\x1Fvalue1\x1F"
            "user.key2\x1Fvalue2\x1F";
        char line[sizeof(orig)];
        const std::size_t line_len = sizeof(orig) - 1;
        EXPECT_EQ(line_len, XATTR_PREFIX_LEN + XATTR_PREFIX_LEN + EXPECTED_XATTRS.len + 2 * EXPECTED_XATTRS.count);

        struct xattrs xattrs;

        memcpy(line, orig, sizeof(orig));
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 0),
                  line + line_len);

        check_contents(xattrs);

        xattrs_cleanup(&xattrs);

        // bad length
        memcpy(line, orig, sizeof(orig));
        line[0] = '?';
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 0),
                  nullptr);

        // too short
        memcpy(line, orig, sizeof(orig));
        line[2] = '0';
        line[3] = '0';
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 0),
                  line + XATTR_PREFIX_LEN + XATTR_PREFIX_LEN);
        xattrs_cleanup(&xattrs);

        // too long
        memcpy(line, orig, sizeof(orig));
        line[0] = 'f';
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 0),
                  nullptr);

        // bad pair count
        memcpy(line, orig, sizeof(orig));
        line[4] = '?';
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 0),
                  nullptr);

        // pair count too low
        memcpy(line, orig, sizeof(orig));
        line[7] = '0';
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 0),
                  line + XATTR_PREFIX_LEN + XATTR_PREFIX_LEN);
        EXPECT_EQ(xattrs.count, (std::size_t) 0);
        xattrs_cleanup(&xattrs);

        // pair count too high
        memcpy(line, orig, sizeof(orig));
        line[4] = 'f';
        EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, XATTRDELIM, 0),
                  line + line_len);
        EXPECT_EQ(xattrs.count, EXPECTED_XATTRS.count); // still got expected count
        xattrs_cleanup(&xattrs);

        // serialized data with no pairs
        char empty[] = "00000000";
        EXPECT_EQ(xattrs_from_line(empty, empty + sizeof(empty) - 1,
                                   &xattrs, XATTRDELIM, 0),
                  empty + sizeof(empty) - 1);
        EXPECT_EQ(xattrs.count, (std::size_t) 0);
        xattrs_cleanup(&xattrs);
    }
}
