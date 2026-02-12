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
    const char S_GOOD[] = "SELECT name FROM " SUMMARY ";";
    const char S_BAD[]  = "INSERT INTO " SUMMARY "(name) VALUES ('');";
    const char E_GOOD[] = "SELECT name FROM " ENTRIES ";";
    const char E_BAD[]  = "INSERT INTO " ENTRIES "(name) VALUES ('');";

    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    in.types.print_tlv = 1;

    for(const char *S : {(const char *) nullptr, S_GOOD, S_BAD}) {
        in.sql.sum.data = S?(char *) S:nullptr;
        in.sql.sum.len = S?strlen(S):0;

        for(const char *E : {(const char *) nullptr, E_GOOD, E_BAD}) {
            in.sql.ent.data = E?(char *) E:nullptr;
            in.sql.ent.len = E?strlen(E):0;

            /* Bad SQL -> return -1 */
            const int expected = -((S == S_BAD) || (E == E_BAD));

            EXPECT_EQ(handle_sql(&in), expected);

            free(in.types.ent);
            in.types.ent = nullptr;
            free(in.types.sum);
            in.types.sum = nullptr;
            free(in.types.tsum);
            in.types.tsum = nullptr;
        }
    }

    input_fini(&in);
}

TEST(handle_sql, aggregation) {
    struct input in;
    ASSERT_EQ(input_init(&in), &in);
    in.types.print_tlv = 1;

    const char I[]      = "CREATE TABLE;";
    const char J[]      = "INSERT INTO";
    const char K_GOOD[] = "CREATE TABLE agg(i INT)";
    const char K_BAD[]  = "CREATE TABLE";
    const char G_GOOD[] = "SELECT i FROM agg";
    const char G_BAD[]  = "INSERT INT agg (i) VALUES (0);";

    in.sql.init = REFSTR(I, strlen(I));
    in.sql.intermediate = REFSTR(J, strlen(J));

    in.sql.init_agg = REFSTR(K_GOOD, strlen(K_GOOD));
    in.sql.agg = REFSTR(G_GOOD, strlen(G_GOOD));
    EXPECT_EQ(handle_sql(&in), 0);

    free(in.types.agg);
    in.types.agg = nullptr;

    in.sql.agg = REFSTR(G_BAD, strlen(G_BAD));
    EXPECT_EQ(handle_sql(&in), -1);

    in.sql.init_agg = REFSTR(K_BAD, strlen(K_BAD));
    EXPECT_EQ(handle_sql(&in), -1);

    input_fini(&in);
}
