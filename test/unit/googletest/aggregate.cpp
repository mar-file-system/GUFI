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



#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <limits>
#include <random>
#include <string>

#include <gtest/gtest.h>

extern "C" {

#include "gufi_query/aggregate.h"

}

TEST(gufi_query, aggregate) {
    const std::string CREATE_SOURCE = "CREATE TABLE source(value INTEGER);";
    const std::string INSERT_SOURCE = "INSERT INTO  source VALUES (%" PRIu32 ");";

    // input arguments
    const std::string I = "CREATE TABLE intermediate(tot INTEGER);";
    const std::string E = "INSERT INTO  intermediate SELECT SUM(value) FROM source;";
    const std::string K = "CREATE TABLE aggregate(partial INTEGER);";
    const std::string J = "INSERT INTO  aggregate(partial) SELECT tot from intermediate;";
    const std::string G = "SELECT SUM(partial) FROM aggregate";

    const std::string BAD_SQL = "SELECT";

    std::random_device rd;
    std::mt19937 gen(rd());

    const size_t row_count = std::uniform_int_distribution <uint32_t> (1, 8)(gen);

    struct input in;
    in.output                = STDOUT;
    in.maxthreads            = std::uniform_int_distribution <uint32_t> (1, 8)(gen);
    in.delim                 = ' ';
    in.sql.init.data         = I.c_str(); in.sql.init.len = strlen(in.sql.init.data);
    in.sql.ent.data          = E.c_str(); in.sql.ent.len = strlen(in.sql.ent.data);
    in.sql.init_agg.data     = K.c_str(); in.sql.init_agg.len = strlen(in.sql.init_agg.data);
    in.sql.intermediate.data = J.c_str(); in.sql.intermediate.len = strlen(in.sql.intermediate.data);
    in.sql.agg.data          = G.c_str(); in.sql.agg.len = strlen(in.sql.agg.data);
    in.output_buffer_size    = 0;
    in.skip.data             = nullptr;

    PoolArgs_t pa;
    ASSERT_EQ(PoolArgs_init(&pa, &in, nullptr), 0);

    std::uniform_int_distribution <uint32_t> dist(0, std::numeric_limits<uint32_t>::max());

    uint64_t sum = 0;
    for(size_t i = 0; i < (size_t) in.maxthreads; i++) {
        ThreadArgs_t *ta = &(pa.ta[i]);

        // create source tables
        EXPECT_EQ(sqlite3_exec(ta->outdb, CREATE_SOURCE.c_str(), nullptr, nullptr, nullptr), SQLITE_OK);

        // add data to the source tables
        for(size_t j = 0; j < row_count; j++) {
            const uint32_t value = dist(gen);

            char sql[MAXSQL];
            snprintf(sql, sizeof(sql), INSERT_SOURCE.c_str(), value);
            EXPECT_EQ(sqlite3_exec(ta->outdb, sql, nullptr, nullptr, nullptr), SQLITE_OK);

            // in-memory result
            sum += (uint64_t) value;
        }

        // run query and place results into intermediate table
        EXPECT_EQ(sqlite3_exec(ta->outdb, in.sql.ent.data, nullptr, nullptr, nullptr), SQLITE_OK);
    }

    // set up the aggregation table
    Aggregate_t aggregate;

    // init with bad SQL statement
    in.sql.init_agg.data = BAD_SQL.c_str(); in.sql.init_agg.len = strlen(in.sql.init_agg.data);
    ASSERT_EQ(aggregate_init(&aggregate, &in), nullptr);

    // init with good SQL statement
    in.sql.init_agg.data = K.c_str(); in.sql.init_agg.len = strlen(in.sql.init_agg.data);
    ASSERT_NE(aggregate_init(&aggregate, &in), nullptr);

    // replace stdout with in-memory file
    char db_sum_str[1024] = {0};
    aggregate.outfile = fmemopen(db_sum_str, sizeof(db_sum_str), "w+b");
    ASSERT_NE(aggregate.outfile, nullptr);

    // run bad intermediate SQL statement
    in.sql.intermediate.data = BAD_SQL.c_str(); in.sql.intermediate.len = strlen(in.sql.intermediate.data);
    EXPECT_NO_THROW(aggregate_intermediate(&aggregate, &pa, &in));

    // run intermediate SQL statement to move data into aggregate table
    in.sql.intermediate.data = J.c_str(); in.sql.intermediate.len = strlen(in.sql.intermediate.data);
    EXPECT_NO_THROW(aggregate_intermediate(&aggregate, &pa, &in));

    // run aggregate SQL to get final result
    EXPECT_EQ(aggregate_process(&aggregate, &in), 0);

    // run a bad aggregate SQL statement
    in.sql.agg.data = BAD_SQL.c_str(); in.sql.agg.len = strlen(in.sql.agg.data);
    EXPECT_EQ(aggregate_process(&aggregate, &in), -1);

    // cleanup
    aggregate_fin(&aggregate, &in);
    PoolArgs_fin(&pa, in.maxthreads);

    // let cleanup flush buffers
    char sum_str[1024];
    const size_t sum_str_len = snprintf(sum_str, sizeof(sum_str), "%" PRIu64, sum);
    EXPECT_EQ(memcmp(db_sum_str, sum_str, sum_str_len), 0);
}
