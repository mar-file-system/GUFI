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



#ifndef ADDQUERYFUNCS_H
#define ADDQUERYFUNCS_H

#include <sqlite3.h>

#include "bf.h"
#include "histogram.h"

#ifdef __cplusplus
extern "C" {
#endif

/* list of functions to add to a SQLite3 db handle that do not have user data/context */

void uidtouser(sqlite3_context *context, int argc, sqlite3_value **argv);
void gidtogroup(sqlite3_context *context, int argc, sqlite3_value **argv);
void modetotxt(sqlite3_context *context, int argc, sqlite3_value **argv);
void sqlite3_strftime(sqlite3_context *context, int argc, sqlite3_value **argv);
void blocksize(sqlite3_context *context, int argc, sqlite3_value **argv);
void human_readable_size(sqlite3_context *context, int argc, sqlite3_value **argv);
void sqlite_basename(sqlite3_context *context, int argc, sqlite3_value **argv);
void stdev_step(sqlite3_context *context, int argc, sqlite3_value **argv);
void stdevs_final(sqlite3_context *context);
void stdevp_final(sqlite3_context *context);
void median_step(sqlite3_context *context, int argc, sqlite3_value **argv);
void median_final(sqlite3_context *context);

static inline int addqueryfuncs(sqlite3 *db) {
    return !(
        (sqlite3_create_function(db,   "uidtouser",           1,   SQLITE_UTF8,
                                 NULL, &uidtouser,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "gidtogroup",          1,   SQLITE_UTF8,
                                 NULL, &gidtogroup,                NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "modetotxt",           1,   SQLITE_UTF8,
                                 NULL, &modetotxt,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "strftime",            2,   SQLITE_UTF8,
                                 NULL, &sqlite3_strftime,          NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "blocksize",           2,   SQLITE_UTF8,
                                 NULL, &blocksize,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "human_readable_size", 1,   SQLITE_UTF8,
                                 NULL, &human_readable_size,       NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "basename",            1,   SQLITE_UTF8,
                                 NULL, &sqlite_basename,           NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevs",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevs_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevp",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevp_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "median",              1,   SQLITE_UTF8,
                                 NULL, NULL,  median_step,         median_final) == SQLITE_OK) &&
        addhistfuncs(db)
        );
}

int addqueryfuncs_with_context(sqlite3 *db, struct work *work);

#ifdef __cplusplus
}
#endif

#endif
