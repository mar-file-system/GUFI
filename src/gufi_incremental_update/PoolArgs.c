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
#include <stdlib.h>
#include <string.h>

#include <trie.h>

#include "gufi_incremental_update/PoolArgs.h"

static int setup_suspect_file(struct PoolArgs *pa) {
    pa->suspectdirs.inodes = trie_alloc();
    pa->suspectdirs.min = (ino_t) -1;
    pa->suspectdirs.max = 0;

    pa->suspectfl.inodes = trie_alloc();
    pa->suspectfl.min = (ino_t) -1;
    pa->suspectfl.max = 0;

    if (pa->in.suspectfile > 0) {
        FILE *f = fopen(pa->in.insuspect.data, "r"); /* --suspect-file */
        if(!f) {
            fprintf(stderr, "Can't open suspect file %s\n", pa->in.insuspect.data);
            return 1;
        }

        /* read suspect file and insert inodes into tries depending on type */
        char inode_str[32];  /* inode in string form */
        char type;           /* d/f/l */
        while (fscanf(f, "%s %c", inode_str, &type) == 2) {
            ino_t inode = 0;
            if (sscanf(inode_str, "%" STAT_ino, &inode) != 1) {
                continue;
            }

            if (type == 'd') {
                MIN_ASSIGN_LHS(pa->suspectdirs.min, inode);
                MAX_ASSIGN_LHS(pa->suspectdirs.max, inode);

                trie_insert(pa->suspectdirs.inodes, inode_str, strlen(inode_str), NULL, NULL);
            }
            else if ((type == 'f') || (type == 'l')) {
                MIN_ASSIGN_LHS(pa->suspectfl.min, inode);
                MAX_ASSIGN_LHS(pa->suspectfl.max, inode);

                trie_insert(pa->suspectfl.inodes, inode_str, strlen(inode_str), NULL, NULL);
            }
        }

        fclose(f);
    }

    return 0;
}

int PoolArgs_init(struct PoolArgs *pa) {
    /* use real index path internally */
    pa->index.data = realpath(pa->orig.index.data, NULL);
    if (!pa->index.data) {
        const int err = errno;
        fprintf(stderr, "Error: Could not get realpath of index \"%s\": %s (%d)\n",
                pa->orig.index.data, strerror(err), err);
        return 1;
    }
    pa->index.len = strlen(pa->index.data);
    pa->parent_len.index = trailing_match_index(pa->index.data, pa->index.len, "/", 1);

    /* use real tree path internally */
    pa->tree.data = realpath(pa->orig.tree.data, NULL);
    if (!pa->tree.data) {
        const int err = errno;
        fprintf(stderr, "Error: Could not get realpath of tree \"%s\": %s (%d)\n",
                pa->orig.tree.data, strerror(err), err);
        return 1;
    }
    pa->tree.len = strlen(pa->tree.data);
    pa->parent_len.tree = trailing_match_index(pa->tree.data, pa->tree.len, "/", 1);

    if (setup_suspect_file(pa) != 0) {
        return 1;
    }

    pa->pool = QPTPool_init(pa->in.maxthreads, pa);
    if (QPTPool_start(pa->pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        return 1;
    }

    return 0;
}

void PoolArgs_fini(struct PoolArgs *pa) {
    QPTPool_stop(pa->pool);
    QPTPool_destroy(pa->pool);
    trie_free(pa->suspectfl.inodes);
    trie_free(pa->suspectdirs.inodes);
    str_free_existing(&pa->index);
    str_free_existing(&pa->tree);
    input_fini(&pa->in);
}
