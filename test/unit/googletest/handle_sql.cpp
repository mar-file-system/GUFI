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



#include <cstdlib>
#include <cstring>

#include <gtest/gtest.h>

extern "C" {

#include "bf.h"
#include "dbutils.h"
#include "gufi_query/handle_sql.h"

}

TEST(handle_sql, no_aggregation) {
    // not testing treesummary for now
    const char S[] = "SELECT name FROM " SUMMARY ";";
    const char E[] = "SELECT name FROM " ENTRIES ";";

    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    in.types.print_tlv_ent = REFSTR("0001:3:0004:name", (std::size_t) 16);

    in.sql.sum.data = (char *) S;
    in.sql.sum.len = strlen(S);

    in.sql.ent.data = (char *) E;
    in.sql.ent.len = strlen(E);

    /* SQL is executed later; handle_sql only installs supplied types. */
    EXPECT_EQ(handle_sql(&in), 0);

    EXPECT_EQ(in.types.tsum,   nullptr); // doesn't exist
    EXPECT_EQ(in.types.sum,    nullptr); // no metadata provided
    EXPECT_EQ(in.types.ent[0], SQLITE_TEXT);

    input_fini(&in);
}

TEST(handle_sql, aggregation) {
    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    in.types.print_tlv_agg = REFSTR("0001:3:0004:name", (std::size_t) 16);

    const char I[] = "CREATE TABLE;";
    const char J[] = "INSERT INTO";
    const char K[] = "CREATE TABLE agg(i INT)";
    const char G[] = "SELECT i FROM agg";

    in.sql.init = REFSTR(I, strlen(I));
    in.sql.intermediate = REFSTR(J, strlen(J));

    in.sql.init_agg = REFSTR(K, strlen(K));
    in.sql.agg = REFSTR(G, strlen(G));
    EXPECT_EQ(handle_sql(&in), 0);

    EXPECT_EQ(in.types.agg[0], SQLITE_TEXT);

    input_fini(&in);
}

TEST(handle_sql, invalid_print_tlv_ent) {
    const char *invalid[] = {
        "000g:3:0004:name",  /* invalid count */
        "0001:",             /* truncated descriptor */
        "0001-3:0004:name",  /* missing count separator */
        "0001:6:0004:name",  /* invalid type */
        "0001:3:000g:name",  /* invalid length */
        "0001:3:0004Xname",  /* missing length separator */
        "0001:3:0005:name",  /* truncated column name */
        "0001:3:0004:namex", /* trailing data */
    };

    for (const char *metadata : invalid) {
        struct input in;
        ASSERT_EQ(input_init(&in), &in);
        in.types.print_tlv_ent = REFSTR(metadata, strlen(metadata));
        in.sql.ent = REFSTR("SELECT 1", (std::size_t) 8);

        EXPECT_EQ(handle_sql(&in), -1) << metadata;
        input_fini(&in);
    }
}

TEST(handle_sql, aggregation_no_metadata) {
    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    // no print_tlv_agg metadata

    const char I[] = "CREATE TABLE;";
    const char J[] = "INSERT INTO";
    const char K[] = "CREATE TABLE agg(i INT)";
    const char G[] = "SELECT i FROM agg";

    in.sql.init = REFSTR(I, strlen(I));
    in.sql.intermediate = REFSTR(J, strlen(J));

    in.sql.init_agg = REFSTR(K, strlen(K));
    in.sql.agg = REFSTR(G, strlen(G));
    EXPECT_EQ(handle_sql(&in), 0);

    // types.agg stays nullptr since no metadata was provided
    EXPECT_EQ(in.types.agg, nullptr);

    input_fini(&in);
}

TEST(handle_sql, tsum_no_metadata) {
    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    // no print_tlv_tsum metadata

    const char T[] = "SELECT * FROM treesummary;";
    in.sql.tsum = REFSTR(T, strlen(T));

    EXPECT_EQ(handle_sql(&in), 0);

    // types.tsum stays nullptr since no metadata was provided
    EXPECT_EQ(in.types.tsum, nullptr);

    input_fini(&in);
}

TEST(handle_sql, sum_no_metadata) {
    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    // no print_tlv_sum metadata

    const char S[] = "SELECT * FROM summary;";
    in.sql.sum = REFSTR(S, strlen(S));

    EXPECT_EQ(handle_sql(&in), 0);

    // types.sum stays nullptr since no metadata was provided
    EXPECT_EQ(in.types.sum, nullptr);

    input_fini(&in);
}

TEST(handle_sql, ent_no_metadata) {
    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    // no print_tlv_ent metadata

    const char E[] = "SELECT * FROM entries;";
    in.sql.ent = REFSTR(E, strlen(E));

    EXPECT_EQ(handle_sql(&in), 0);

    // types.ent stays nullptr since no metadata was provided
    EXPECT_EQ(in.types.ent, nullptr);

    input_fini(&in);
}

TEST(handle_sql, output_not_stdout) {
    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    in.output = OUTDB; // not STDOUT or OUTFILE

    // validate() requires -I when output is OUTDB
    in.sql.init = REFSTR("CREATE TABLE x(i INT);", (std::size_t) 21);

    // metadata provided but output is not STDOUT/OUTFILE, so gen_types returns 0
    in.types.print_tlv_ent = REFSTR("0001:3:0004:name", (std::size_t) 16);
    in.sql.ent = REFSTR("SELECT * FROM entries;", (std::size_t) 22);

    EXPECT_EQ(handle_sql(&in), 0);

    // types.ent stays nullptr since gen_types returned early
    EXPECT_EQ(in.types.ent, nullptr);

    input_fini(&in);
}

TEST(handle_sql, print_tlv_modes) {
    for (const char *sql : {"tsum", "sum", "ent", "agg"}) {
        struct input in;
        ASSERT_EQ(input_init(&in), &in);

        if (!strncmp(sql, "tsum", (std::size_t) 4)) {
            in.types.print_tlv_tsum = REFSTR("0001:3:0004:name", (std::size_t) 16);
            in.sql.tsum         = REFSTR("SELECT 1", (std::size_t) 8);
        }
        else if (!strncmp(sql, "sum", (std::size_t) 4)) {
            in.types.print_tlv_sum = REFSTR("0001:3:0004:name", (std::size_t) 16);
            in.sql.sum          = REFSTR("SELECT 1", (std::size_t) 8);
        }
        else if (!strncmp(sql, "ent", (std::size_t) 4)) {
            in.types.print_tlv_ent = REFSTR("0001:3:0004:name", (std::size_t) 16);
            in.sql.ent          = REFSTR("SELECT 1", (std::size_t) 8);
        }
        else {
            in.types.print_tlv_agg = REFSTR("0001:3:0004:name", (std::size_t) 16);
            in.sql.init         = REFSTR("CREATE TABLE x(i INT)", (std::size_t) 21);
            in.sql.intermediate = REFSTR("INSERT INTO x", (std::size_t) 13);
            in.sql.init_agg     = REFSTR("CREATE TABLE agg(i INT)", (std::size_t) 24);
            in.sql.agg          = REFSTR("SELECT i FROM agg", (std::size_t) 17);
        }

        EXPECT_EQ(handle_sql(&in), 0) << sql;

        int *types = nullptr;
        if (!strncmp(sql, "tsum", (std::size_t) 4)) {
            types = in.types.tsum;
        }
        else if (!strncmp(sql, "sum", (std::size_t) 4)) {
            types = in.types.sum;
        }
        else if (!strncmp(sql, "ent", (std::size_t) 4)) {
            types = in.types.ent;
        }
        else if (!strncmp(sql, "agg", (std::size_t) 4)) {
            types = in.types.agg;
        }
        ASSERT_NE(types, nullptr);
        EXPECT_EQ(types[0], SQLITE_TEXT);

        if (strncmp(sql, "tsum", (std::size_t) 4)) {
            EXPECT_EQ(in.types.tsum, nullptr);
        }
        if (strncmp(sql, "sum", (std::size_t) 4)) {
            EXPECT_EQ(in.types.sum, nullptr);
        }
        if (strncmp(sql, "ent", (std::size_t) 4)) {
            EXPECT_EQ(in.types.ent, nullptr);
        }
        if (strncmp(sql, "agg", (std::size_t) 4)) {
            EXPECT_EQ(in.types.agg, nullptr);
        }

        input_fini(&in);
    }

    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    EXPECT_EQ(handle_sql(&in), 0);
    input_fini(&in);
}
