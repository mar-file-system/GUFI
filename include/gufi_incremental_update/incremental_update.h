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



#ifndef GUFI_INCREMENTAL_UPDATE_H
#define GUFI_INCREMENTAL_UPDATE_H

#include <dirent.h>
#include <stddef.h>

#include "bf.h"

#include "gufi_incremental_update/PoolArgs.h"

/* directory for placing update databases and moved directories */
int setup_parking_lot(const char *path);
int cleanup_parking_lot(const char *path, const int created);

/*
 * insert records into the per-thread readdirplus table/db
 *
 * these will be merged into one at the end
 *
 * used by gen_index_snapshot and find_suspects
 */
int insert_record(struct work *work, struct entry_data *ed,
                  sqlite3_stmt *res, const size_t offset_name);

/* *********************************************************** */
/* walk the old index and generate a readdirplus table in <snapshot>.index */
size_t gen_index_snapshot_name(struct PoolArgs *pa, char *name, const size_t name_size);
int gen_index_snapshot(struct PoolArgs *pa, struct work *work); /* TODO: reuse gufi_query code */
/* *********************************************************** */

/* *********************************************************** */
/*
 * If a directory is suspected to have changed, reindex that one directory
 * If the index is separate from the tree, place the database file under <parking lot>/<dir inode>
 * If the index is in the tree, replace the existing database file (no need to call incremental_update)
 *
 * TODO: reuse gufi_dir2index code
 */
int reindex_dir(struct PoolArgs *pa,
                struct work *work, struct entry_data *ed,
                DIR *dir);

/* walk the current index and generate a readdirplus table in <snapshot>.tree */
size_t gen_tree_snapshot_name(struct PoolArgs *pa, char *name, const size_t name_size);
int find_suspects(struct PoolArgs *pa, struct work *work);
/* *********************************************************** */

/* do the incremental update after getting snapshots from the original index and updated tree */
int incremental_update(struct PoolArgs *pa);

#endif
