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



#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#include "bf.h"
#include "print.h"
#include "utils.h"

#include "gufi_query/gufi_query_run.h"

static void sub_help(void) {
   printf("GUFI_tree        find GUFI tree here\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
    /* process input args - all programs share the common 'struct input', */
    /* but allow different fields to be filled at the command-line. */
    /* Callers provide the options-string for get_opt(), which will */
    /* control which options are parsed for each program. */
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_THREADS,

        FLAG_GLOBAL_DB,

        /* SQL flags */
        FLAG_SQL_INIT,
        FLAG_SQL_TSUM, FLAG_SQL_SUM, FLAG_SQL_ENT,
        FLAG_SQL_INTERM, FLAG_SQL_CREATE_AGG, FLAG_SQL_AGG,
        FLAG_SQL_FIN,

        /* SQL processing/tree walk flags */
        FLAG_DIR_MATCH_UID, FLAG_DIR_MATCH_GID, FLAG_PROCESS_SQL,
        FLAG_MIN_LEVEL, FLAG_MAX_LEVEL, FLAG_PATH_LIST,
        FLAG_PATH, FLAG_XATTRS, FLAG_QUERY_XATTRS,
        FLAG_EXTERNAL_ATTACH, FLAG_EXTERNAL_COPY,
        FLAG_INTERNAL_COPY, FLAG_PLUGIN,

        /* miscellaneous flags */
        #if ALLOW_DB_WRITES
        FLAG_READ_WRITE,
        #endif

        FLAG_KEEP_MATIME, FLAG_SKIP_FILE,

        /* output flags */
        FLAG_DELIM, FLAG_NEWLINE, FLAG_SUPPRESS_NEWLINE,
        FLAG_OUTPUT_FILE, FLAG_OUTPUT_DB, FLAG_PRINT_TLV,
        FLAG_SETUP_RES_COL_TYPES, FLAG_TERSE, FLAG_NO_PRINT_ERRNO,
        FLAG_NO_PRINT_SQL_ON_ERR,

        /* memory usage flags  */
        FLAG_OUTPUT_BUFFER_SIZE, FLAG_TARGET_MEMORY, FLAG_SWAP_PREFIX,
        #ifdef HAVE_ZLIB
        FLAG_COMPRESS,
        #endif

        FLAG_END
    };

    struct input in;
    process_args_and_maybe_exit(options, 0, "GUFI_tree...", &in);

    PrintArgs_t print_args = {
        .delim = in.delim,
        .newline = in.newline,
        .suppress_newline = in.suppress_newline,
    };
    in.output_cb = print_parallel;
    in.output_cb_arg = &print_args;

    return gufi_query_run(&in);
}
