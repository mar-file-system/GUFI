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
#include <string.h>
#include <unistd.h>

#include "bf.h"
#include "BottomUp.h"
#include "debug.h"
#include "utils.h"

extern int errno;

/* Remove all non-subdirectories in the   */
/* current directory. Then remove itself. */
/* Subdirectories are already gone, so    */
/* they don't have to processed at the    */
/* current level.                         */
void rm_dir(void * args timestamp_sig) {
    struct BottomUp * dir = (struct BottomUp *) args;

    char db_name[MAXPATH];
    SNPRINTF(db_name, MAXPATH, "%s/" DBNAME, dir->name);

    sll_loop(&dir->subnondirs, node) {
        struct BottomUp * entry = (struct BottomUp *) sll_node_data(node);
        if (unlink(entry->name) != 0) {
            fprintf(stderr, "Warning: Failed to delete \"%s\": %s\n", entry->name, strerror(errno));
        }
    }

    if (rmdir(dir->name) != 0) {
        fprintf(stderr, "Warning: Failed to remove \"%s\": %s\n", dir->name, strerror(errno));
    }
}

void sub_help() {
   printf("directory        directory to delete\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:", 1, "directory ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    #ifdef DEBUG
    #ifdef PER_THREAD_STATS
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    epoch = since_epoch(&now);
    #endif

    timestamp_init(timestamp_buffers, in.maxthreads + 1, 1024 * 1024, NULL);
    #endif

    const int rc = parallel_bottomup(argv + idx, argc - idx,
                                     in.maxthreads,
                                     sizeof(struct BottomUp), rm_dir,
                                     1,
                                     NULL
                                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                     , timestamp_buffers
                                     #endif
        );

    timestamp_destroy(timestamp_buffers);

    return rc;
}
