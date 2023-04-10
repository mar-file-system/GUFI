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



#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>

#include "bf.h"
#include "utils.h"
#include "dbutils.h"
#include "QueuePerThreadPool.h"

struct PoolArgs {
    struct input in;
    trie_t *skip;
    struct sum *sums;
};

static int create_tables(const char *name, sqlite3 *db, void *args) {
    struct input *in = (struct input *) args;

    printf("writetsum %d\n", in->writetsum);
    if ((create_table_wrapper(name, db, "tsql",        tsql)        != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqldir",   vtssqldir)   != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqluser",  vtssqluser)  != SQLITE_OK) ||
        (create_table_wrapper(name, db, "vtssqlgroup", vtssqlgroup) != SQLITE_OK)) {
        return -1;
    }

    return 0;
}

static int treesummary_exists(void *args, int count, char **data, char **columns) {
    int *trecs = (int *) args;
    (*trecs)++;
    return 0;
}

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args)
{
    struct work *passmywork = data;
    struct entry_data ed;
    char dbname[MAXPATH];
    DIR *dir = NULL;
    sqlite3 *db = NULL;

    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;
    trie_t *skip = pa->skip;
    memset(&ed, 0, sizeof(ed));

    if (!(dir = opendir(passmywork->name))) {
        goto out_free;
    }

    if (lstat(passmywork->name, &ed.statuso) != 0) {
        goto out_free;
    }

    if (in->printing || in->printdir) {
        ed.type = 'd';
        printits(in, passmywork, &ed, id);
    }

    SNPRINTF(dbname, MAXPATH, "%s/%s", passmywork->name, DBNAME);
    if ((db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 1
                     , NULL, NULL
                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                     , NULL, NULL
                     , NULL, NULL
                     #endif
             ))) {
        int trecs = 0;

        /* ignore errors */
        sqlite3_exec(db, "SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'treesummary\';",
                     treesummary_exists, &trecs, NULL);

        struct sum sumin;
        zeroit(&sumin);

        if (trecs < 1) {
            /*
             * this directory does not have a tree summary
             * table, so need to descend to collect
             */
            descend(ctx, id, pa,
                    in, passmywork, ed.statuso.st_ino,
                    dir, skip, 0, 0, processdir,
                    NULL, NULL, NULL, NULL, NULL, NULL);
            querytsdb(passmywork->name, &sumin, db, 0);
        } else {
            querytsdb(passmywork->name, &sumin, db, 1);
        }

        tsumit(&sumin, &pa->sums[id]);
    }

    closedb(db);
    closedir(dir);

  out_free:
    if (passmywork->recursion_level == 0) {
        free(passmywork);
    }

    return 0;
}

int processinit(struct PoolArgs *pa, QPTPool_t *ctx) {
    struct work *mywork = calloc(1, sizeof(struct work));
    struct stat st;

    /* process input directory and put it on the queue */
    mywork->name_len = SNFORMAT_S(mywork->name, MAXPATH, 1, pa->in.name, pa->in.name_len);
    mywork->name_len = trailing_match_index(mywork->name, mywork->name_len, "/", 1);

    lstat(pa->in.name, &st);
    if (access(pa->in.name, R_OK | X_OK)) {
        const int err = errno;
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                pa->in.name, strerror(err));
        free(mywork);
        return 1;
    }
    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "input-dir '%s' is not a directory\n", pa->in.name);
        free(mywork);
        return 1;
    }

    /* skip . and .. only */
    setup_directory_skip(NULL, &pa->skip);

    pa->sums = calloc(pa->in.maxthreads, sizeof(struct sum));
    for(int i = 0; i < pa->in.maxthreads; i++) {
        zeroit(&pa->sums[i]);
    }

    /* push the path onto the queue */
    QPTPool_enqueue(ctx, 0, processdir, mywork);

    return 0;
}

int processfin(struct PoolArgs *pa) {
    struct sum sumout;
    zeroit(&sumout);
    for(int i = 0; i < pa->in.maxthreads; i++) {
        tsumit(&pa->sums[i], &sumout);
        sumout.totsubdirs--; /* tsumit adds 1 to totsubdirs each time it's called */
    }
    sumout.totsubdirs--; /* subtract another 1 because starting directory is not a subdirectory of itself */
    free(pa->sums);

    trie_free(pa->skip);

    char dbpath[MAXPATH];
    SNPRINTF(dbpath, MAXPATH, "%s/%s", pa->in.name, DBNAME);

    struct stat st;
    const int rc = lstat(dbpath, &st);

    if (pa->in.writetsum) {
        sqlite3 *tdb = NULL;
        if (!(tdb = opendb(dbpath, SQLITE_OPEN_READWRITE, 1, 1,
                           create_tables, &pa->in
                           #if defined(DEBUG) && defined(PER_THREAD_STATS)
                           , NULL, NULL
                           , NULL, NULL
                           #endif
                  )))
            return -1;
        inserttreesumdb(pa->in.name, tdb, &sumout, 0, 0, 0);
        closedb(tdb);
    }

    if (rc == 0) {
        struct utimbuf utimeStruct;
        utimeStruct.actime  = st.st_atime;
        utimeStruct.modtime = st.st_mtime;
        if(utime(dbpath, &utimeStruct) != 0) {
            const int err = errno;
            fprintf(stderr, "ERROR: utime failed with error number: %d on %s\n", err, dbpath);
            return 1;
        }
    }

    printf("totals:\n");
    printf("totfiles %lld totlinks %lld\n",
           sumout.totfiles, sumout.totlinks);
    printf("totsize %lld\n",
           sumout.totsize);
    printf("minuid %lld maxuid %lld mingid %lld maxgid %lld\n",
           sumout.minuid, sumout.maxuid, sumout.mingid, sumout.maxgid);
    printf("minsize %lld maxsize %lld\n",
           sumout.minsize, sumout.maxsize);
    printf("totltk %lld totmtk %lld totltm %lld totmtm %lld totmtg %lld totmtt %lld\n",
           sumout.totltk, sumout.totmtk, sumout.totltm, sumout.totmtm, sumout.totmtg, sumout.totmtt);
    printf("minctime %lld maxctime %lld\n",
           sumout.minctime, sumout.maxctime);
    printf("minmtime %lld maxmtime %lld\n",
           sumout.minmtime, sumout.maxmtime);
    printf("minatime %lld maxatime %lld\n",
           sumout.minatime, sumout.maxatime);
    printf("minblocks %lld maxblocks %lld\n",
           sumout.minblocks, sumout.maxblocks);
    printf("totxattr %lld\n",
           sumout.totxattr);
    printf("mincrtime %lld maxcrtime %lld\n",
           sumout.mincrtime, sumout.maxcrtime);
    printf("minossint1 %lld maxossint1 %lld totossint1 %lld\n",
           sumout.minossint1, sumout.maxossint1, sumout.totossint1);
    printf("minossint2 %lld maxossint2 %lld totossint2 %lld\n",
           sumout.minossint2, sumout.maxossint2, sumout.totossint2);
    printf("minossint3 %lld maxossint3 %lld totossint3 %lld\n",
           sumout.minossint3, sumout.maxossint3, sumout.totossint3);
    printf("minossint4 %lld maxossint4 %lld totossint4 %lld\n",
           sumout.minossint4, sumout.maxossint4, sumout.totossint4);
    printf("totsubdirs %lld maxsubdirfiles %lld maxsubdirlinks %lld maxsubdirsize %lld\n",
           sumout.totsubdirs, sumout.maxsubdirfiles, sumout.maxsubdirlinks, sumout.maxsubdirsize);

    return 0;
}

void sub_help() {
    printf("GUFI_index        path to GUFI index\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    /*
     * process input args - all programs share the common 'struct input',
     * but allow different fields to be filled at the command-line.
     * Callers provide the options-string for get_opt(), which will
     * control which options are parsed for each program.
     */
    struct PoolArgs pa;
    int idx = parse_cmd_line(argc, argv, "hHPn:s", 1, "GUFI_index", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0)
        return 1;
    else {
        int retval = 0;
        INSTALL_STR(pa.in.name, argv[idx++], MAXPATH, "GUFI_index");
        pa.in.name_len = strlen(pa.in.name);

        if (retval)
            return retval;
    }

    /* not an error, but you might want to know ... */
    if (!pa.in.writetsum) {
        fprintf(stderr, "WARNING: Not [re]generating tree-summary table without '-s'\n");
    }

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa, NULL, NULL, 0
                                   #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                   , NULL
                                   #endif
        );
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return 1;
    }

    processinit(&pa, pool);

    QPTPool_wait(pool);

    QPTPool_destroy(pool);

    processfin(&pa);

    return 0;
}
