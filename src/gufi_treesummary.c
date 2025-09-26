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

static int printits(struct input *in, struct work *pwork, struct entry_data *ed, FILE *out) {
  fprintf(out, "%s%c",              pwork->name,               in->delim);
  fprintf(out, "%c%c",              ed->type,                  in->delim);
  fprintf(out, "%" STAT_ino   "%c", pwork->statuso.st_ino,     in->delim);
  fprintf(out, "%" STAT_mode  "%c", pwork->statuso.st_mode,    in->delim);
  fprintf(out, "%" STAT_nlink "%c", pwork->statuso.st_nlink,   in->delim);
  fprintf(out, "%" STAT_uid   "%c", pwork->statuso.st_uid,     in->delim);
  fprintf(out, "%" STAT_gid   "%c", pwork->statuso.st_gid,     in->delim);
  fprintf(out, "%" STAT_size  "%c", pwork->statuso.st_size,    in->delim);
  fprintf(out, "%" STAT_bsize "%c", pwork->statuso.st_blksize, in->delim);
  fprintf(out, "%" STAT_blocks"%c", pwork->statuso.st_blocks,  in->delim);
  fprintf(out, "%ld%c",             pwork->statuso.st_atime,   in->delim);
  fprintf(out, "%ld%c",             pwork->statuso.st_mtime,   in->delim);
  fprintf(out, "%ld%c",             pwork->statuso.st_ctime,   in->delim);

/* we need this field even if its not populated for gufi_trace2index */
/*
  if (!strncmp(pwork->type,"l",1)) {
    fprintf(out, "%s%c", ed->linkname,in->delim);
  }
*/
  fprintf(out, "%s%c", ed->linkname,in->delim);

/* we need this field even if its not populated for gufi_trace2index */
/*
  if (ed->xattrs > 0) {
    //printf("xattr: ");
    fprintf(out,"%s%c",ed->xattr,in->delim);
  }
*/
  /* fwrite(ed->xattrs, sizeof(char), ed->xattrs_len, out); */
  fprintf(out,"%c",in->delim);

  /* this one is for create time which posix doesnt have */
  fprintf(out,"%c", in->delim);
  /* moved this to end because we would like to use this for input to gufi_trace2index load from file */
  fprintf(out, "%lld%c", pwork->pinode, in->delim);
  fprintf(out, "%d%c", ed->suspect, in->delim);
  fprintf(out,"\n");
  return 0;
}

struct PoolArgs {
    struct input in;
    struct sum *sums; /* per thread summary data */
};

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct work *passmywork = (struct work *) data;

    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));
    if (lstat_wrapper(passmywork) != 0) {
        goto out_free;
    }

    DIR *dir = opendir(passmywork->name);
    if (!dir) {
        goto out_free;
    }

    if (pa->in.printdir) {
        ed.type = 'd';
        printits(&pa->in, passmywork, &ed, stdout);
    }

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 2,
               passmywork->name, passmywork->name_len,
               "/" DBNAME, DBNAME_LEN + 1);

    sqlite3 *db = opendb(dbname, SQLITE_OPEN_READONLY, 1, 0, NULL, NULL);
    if (db) {
        struct sum sum;
        zeroit(&sum);

        int rollupscore = 0;
        get_rollupscore(db, &rollupscore);
        if (rollupscore != 0) {
            /*
             * this directory has been rolled up, so all information is
             * available here: compute the treesummary, no need to go
             * further down
             *
             * cannot operate within sqlite since this function is
             * aggregating into memory
             *
             * this ignores all treesummary tables since all summary
             * tables are immediately available
             *
             * -1 because a directory is not a subdirectory of itself
             */
            sum.totsubdirs = querytsdb(passmywork->name, &sum, db, 0) - 1;
        }
        else {
            /*
             * should not error; if this does error,
             * treat as no treesummary table
             */
            int trecs = 0;
            sqlite3_exec(db, TREESUMMARY_EXISTS,
                         treesummary_exists_callback, &trecs, NULL);

            if ((trecs < 1) || (passmywork->level == 0)) {
                /*
                 * this directory does not have a treesummary table,
                 * so need to descend to collect from summary table
                 *
                 * force descent at level 0 so that if the root directory
                 * already has a treesummary table, the existing data is
                 * not used to generate the new treesummary table
                 */
                descend(ctx, id, pa, &pa->in, passmywork, dir, 0,
                        processdir, NULL, NULL, NULL);

                /* add summary data from this directory */
                querytsdb(passmywork->name, &sum, db, 0);
            } else {
                /* add treesummary data from this directory and don't descend */
                querytsdb(passmywork->name, &sum, db, 1);
            }
        }

        tsumit(&sum, &pa->sums[id]);
    }

    closedb(db);
    closedir(dir);

  out_free:
    free(passmywork);

    return 0;
}

static int compute_treesummary(struct PoolArgs *pa) {
    struct sum sumout;
    zeroit(&sumout);
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        tsumit(&pa->sums[i], &sumout);
        sumout.totsubdirs--; /* tsumit adds 1 to totsubdirs each time it's called */
    }
    if (sumout.totsubdirs) {
        sumout.totsubdirs--; /* subtract another 1 because starting directory is not a subdirectory of itself */
    }

    char dbname[MAXPATH];
    SNFORMAT_S(dbname, sizeof(dbname), 2,
               pa->in.name.data, pa->in.name.len,
               "/" DBNAME, DBNAME_LEN + 1);

    struct stat st;
    int rc = lstat(dbname, &st);

    if (!pa->in.dry_run) {
        sqlite3 *tdb = opendb(dbname, SQLITE_OPEN_READWRITE, 1, 0, create_treesummary_tables, &pa->in);
        if (tdb) {
            inserttreesumdb(pa->in.name.data, tdb, &sumout, 0, 0, 0);
        }
        else {
            rc = 1;
        }

        closedb(tdb);
    }

    if (rc == 0) {
        struct utimbuf utimeStruct;
        utimeStruct.actime  = st.st_atime;
        utimeStruct.modtime = st.st_mtime;
        if(utime(dbname, &utimeStruct) != 0) {
            const int err = errno;
            fprintf(stderr, "ERROR: utime failed with error number: %d on %s\n", err, dbname);
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
    printf("totzero %lld totltk %lld totmtk %lld totltm %lld totmtm %lld totmtg %lld totmtt %lld\n",
           sumout.totzero, sumout.totltk, sumout.totmtk, sumout.totltm, sumout.totmtm, sumout.totmtg, sumout.totmtt);
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
    printf("totextdbs %lld\n",
           sumout.totextdbs);

    return 0;
}

static void sub_help(void) {
    printf("GUFI_tree               path to GUFI tree\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    /*
     * process input args - all programs share the common 'struct input',
     * but allow different fields to be filled at the command-line.
     * Callers provide the options-string for get_opt(), which will
     * control which options are parsed for each program.
     */
    const struct option options[] = {
        FLAG_HELP, FLAG_DEBUG, FLAG_VERSION, FLAG_PRINTDIR,
        FLAG_THREADS, FLAG_DELIM, FLAG_DRY_RUN, FLAG_END
    };
    struct PoolArgs pa;
    process_args_and_maybe_exit(options, 1, "GUFI_tree", &pa.in);

    INSTALL_STR(&pa.in.name, argv[idx++]);

    /* not an error, but you might want to know ... */
    if (pa.in.dry_run) {
        fprintf(stderr, "WARNING: Not [re]generating tree-summary table with '-X'\n");
    }

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    pa.sums = calloc(pa.in.maxthreads, sizeof(struct sum));
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        zeroit(&pa.sums[i]);
    }

    struct work *root = new_work_with_name(NULL, 0, pa.in.name.data, pa.in.name.len);
    root->name_len = trailing_non_match_index(root->name, root->name_len, "/", 1);

    QPTPool_enqueue(pool, 0, processdir, root);
    QPTPool_stop(pool);
    QPTPool_destroy(pool);

    compute_treesummary(&pa);
    free(pa.sums);

    input_fini(&pa.in);

    return EXIT_SUCCESS;
}
