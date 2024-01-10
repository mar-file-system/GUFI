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

#include "xattrs.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "gufi_query/xattrs.h"
#ifdef __cplusplus
}
#endif

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
    const char EXPECTED_STR[MAXXATTR] = "user.key1\x1Fuser.key2\x1F";
    const size_t EXPECTED_STR_LEN = 20;

    EXPECT_EQ(EXPECTED_XATTRS.name_len + EXPECTED_XATTRS.count, EXPECTED_STR_LEN);

    char names[MAXXATTR];
    EXPECT_EQ(xattr_get_names(&EXPECTED_XATTRS, names, MAXXATTR, XATTRDELIM),
              (ssize_t) EXPECTED_STR_LEN);
    EXPECT_EQ(memcmp(names, EXPECTED_STR, EXPECTED_STR_LEN), 0);

    // not enough buffer space
    EXPECT_EQ(xattr_get_names(&EXPECTED_XATTRS, names, 0, XATTRDELIM),
              (ssize_t) -1);
}

TEST(xattrs, to_file) {
    const char EXPECTED_STR[MAXXATTR] = "user.key1\x1Fvalue1\x1Fuser.key2\x1Fvalue2\x1F";
    const size_t EXPECTED_STR_LEN = 34;

    char line[MAXXATTR];
    FILE *file = fmemopen(line, MAXXATTR, "wb");

    // bad xattrs struct
    struct xattrs bad_xattrs = EXPECTED_XATTRS;
    bad_xattrs.pairs = nullptr;
    EXPECT_EQ(xattrs_to_file(file, &bad_xattrs, XATTRDELIM), 0);

    EXPECT_EQ(xattrs_to_file(file, &EXPECTED_XATTRS, XATTRDELIM), (int) EXPECTED_STR_LEN);
    fflush(file);

    EXPECT_EQ(memcmp(line, EXPECTED_STR, EXPECTED_STR_LEN), 0);
    fclose(file);
}

TEST(xattrs, from_line) {
    char line[MAXXATTR];
    const size_t line_len = snprintf(line, MAXXATTR, "user.key1\x1Fvalue1\x1Fuser.key2\x1Fvalue2\x1F");
    EXPECT_EQ(line_len, EXPECTED_XATTRS.len + 2 * EXPECTED_XATTRS.count);

    struct xattrs xattrs;
    EXPECT_EQ(xattrs_from_line(line, line + line_len, &xattrs, '\x1F'),
              (int) EXPECTED_XATTRS.count);

    check_contents(xattrs);

    xattrs_cleanup(&xattrs);
}

/* C Standard 6.10.3/C++ Standard 16.3 Macro replacement */
static int bad_xattr_create_views() {
    #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
    size_t query_count = 0;
    #endif

    return xattr_create_views(nullptr
                              #if defined(DEBUG) && defined(CUMULATIVE_TIMES)
                              , &query_count
                              #endif
        );
}

TEST(gufi_query, bad_xattr_create_view) {
    EXPECT_EQ(bad_xattr_create_views(), 1);
}
