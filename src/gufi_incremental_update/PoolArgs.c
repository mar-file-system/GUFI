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

#include "plugin.h"
#include "trie.h"

#include "gufi_incremental_update/PoolArgs.h"

static int setup_suspect_file(struct PoolArgs *pa) {
    pa->suspects.dir.inodes = trie_alloc();
    pa->suspects.dir.min = (ino_t) -1;
    pa->suspects.dir.max = 0;

    pa->suspects.fl.inodes = trie_alloc();
    pa->suspects.fl.min = (ino_t) -1;
    pa->suspects.fl.max = 0;

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
                MIN_ASSIGN_LHS(pa->suspects.dir.min, inode);
                MAX_ASSIGN_LHS(pa->suspects.dir.max, inode);

                trie_insert(pa->suspects.dir.inodes, inode_str, strlen(inode_str), NULL, NULL);
            }
            else if ((type == 'f') || (type == 'l')) {
                MIN_ASSIGN_LHS(pa->suspects.fl.min, inode);
                MAX_ASSIGN_LHS(pa->suspects.fl.max, inode);

                trie_insert(pa->suspects.fl.inodes, inode_str, strlen(inode_str), NULL, NULL);
            }
        }

        fclose(f);
    }

    return 0;
}

int PoolArgs_init(struct PoolArgs *pa) {
    pa->index.parent_len = trailing_match_index(pa->index.path.data, pa->index.path.len, "/", 1);
    pa->tree.parent_len = trailing_match_index(pa->tree.path.data, pa->tree.path.len, "/", 1);

    /* fail early */
    if (check_plugin(pa->in.plugin_ops, PLUGIN_INCREMENTAL) != 1) {
        pa->in.plugin_ops = NULL;
        return 1;
    }

    if (setup_suspect_file(pa) != 0) {
        return 1;
    }

    /* actually initialize after suspect file has been read */
    if (pa->in.plugin_ops->global_init) {
        pa->in.plugin_ops->global_init(NULL);
    }

    pa->ctx = QPTPool_init(pa->in.maxthreads, pa);
    if (QPTPool_start(pa->ctx) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        return 1;
    }

    return 0;
}

void PoolArgs_fini(struct PoolArgs *pa) {
    QPTPool_stop(pa->ctx);
    QPTPool_destroy(pa->ctx);
    if (pa->in.plugin_ops && pa->in.plugin_ops->global_exit) {
        pa->in.plugin_ops->global_exit(NULL);
    }
    trie_free(pa->suspects.fl.inodes);
    trie_free(pa->suspects.dir.inodes);
    input_fini(&pa->in);
}
