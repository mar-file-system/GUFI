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
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"
#include "dbutils.h"
#include "trie.h"
#include "utils.h"
#include "xattrs.h"

struct ThreadArgs {
    pthread_mutex_t mutex;
    FILE *file;
    sqlite3 *db;
    sqlite3_stmt *res;
};

struct PoolArgs {
    struct input in;
    ino_t glsuspectflmin;
    ino_t glsuspectflmax;
    ino_t glsuspectdmin;
    ino_t glsuspectdmax;
    int gltodirmode;

    trie_t *dirs;
    trie_t *fls;

    struct ThreadArgs *ta;
};

enum DFL {
    DFL_DIR,
    DFL_FL,
};

/* search the trie either the directory trie type 0 or the filelink type 1 */
static int searchmyll(struct PoolArgs *pa, ino_t inode, const enum DFL type) {
    trie_t *dst = NULL;
    if (type == DFL_DIR) {
        if (inode < pa->glsuspectdmin) return 0;
        if (inode > pa->glsuspectdmax) return 0;
        dst = pa->dirs;
    }
    else if (type == DFL_FL) {
        if (inode < pa->glsuspectflmin) return 0;
        if (inode > pa->glsuspectflmax) return 0;
        dst = pa->fls;
    }

    char str[256];
    const size_t len = SNPRINTF(str, sizeof(str), "%" STAT_ino, inode);
    return trie_search(dst, str, len, NULL);
}

static int create_readdirplus_tables(const char *name, sqlite3 *db, void *args) {
    (void) args;

    return (create_table_wrapper(name, db, READDIRPLUS, READDIRPLUS_CREATE) != SQLITE_OK);
}

static int insertdbgor(struct work *pwork, struct entry_data *ed, sqlite3_stmt *res)
{
    char *zname = sqlite3_mprintf("%q", pwork->name);
    char *ztype = sqlite3_mprintf("%c", ed->type);
    sqlite3_bind_text (res, 1, zname, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text (res, 2, ztype, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(res, 3, pwork->statuso.st_ino);
    sqlite3_bind_int64(res, 4, pwork->pinode);
    sqlite3_bind_int64(res, 5, ed->suspect);

    const int error = sqlite3_step(res);
    if (error != SQLITE_DONE) {
        fprintf(stderr,  "SQL insertdbgor step: %s error %d err %s\n",
                pwork->name, error, sqlite3_errstr(error));
    }

    sqlite3_free(ztype);
    sqlite3_free(zname);
    sqlite3_reset(res);
    sqlite3_clear_bindings(res);

    return !!error;
}

static int reprocessdir(struct input *in, void *passv, DIR *dir) {
    struct work *passmywork = passv;
    if ((passmywork->stat_called != STAT_NOT_CALLED) && (lstat(passmywork->name, &passmywork->statuso) != 0)) {
        return 1;
    }

    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));

    /* need to fill this in for the directory as we dont need to do this unless we are making a new gufi db */
    if (in->process_xattrs) {
        xattrs_setup(&ed.xattrs);
        xattrs_get(passmywork->name, &ed.xattrs);
    }

    /* open the gufi db for this directory into the parking lot directory the name as the inode of the dir */
    char dbpath[MAXPATH];
    if (in->buildindex == 1) {
        SNPRINTF(dbpath, MAXPATH, "%s/%s", passmywork->name, DBNAME);
    } else {
        SNPRINTF(dbpath, MAXPATH, "%s/%" STAT_ino, in->nameto.data, passmywork->statuso.st_ino);
    }

    /*
     * if we are building a gufi in the src tree and the suspect mode
     * is not zero then we need to wipe it out first
     */
    if (in->buildindex == 1) {
        if (in->suspectmethod > 0) {
            truncate(dbpath, 0);
        }
    }

    sqlite3 *db = opendb(dbpath, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                         1, 1, create_dbdb_tables, NULL);
    if (!db) {
        return -1;
    }

    sqlite3_stmt *res = insertdbprep(db, ENTRIES_INSERT);
    startdb(db);
    struct sum summary;
    zeroit(&summary, in->epoch);

    /* rewind the directory */
    rewinddir(dir);

    /*
     * loop over dirents, if link push it on the queue, if file or
     * link print it, fill up qwork structure for each
     */
    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        if (((len == 1) && !strncmp(entry->d_name, ".",  1)) ||
            ((len == 2) && !strncmp(entry->d_name, "..", 2)))
            continue;

        if (in->buildindex == 1) {
            if ((len == DBNAME_LEN) && !strncmp(entry->d_name, DBNAME, DBNAME_LEN))
                continue;
        }

        struct work *qwork = new_work_with_name(passmywork->name, passmywork->name_len, entry->d_name, len);
        qwork->basename_len = len;

        struct entry_data qwork_ed;
        memset(&qwork_ed, 0, sizeof(qwork_ed));
        qwork->pinode = passmywork->statuso.st_ino;

        try_skip_lstat(entry->d_type, qwork);
        xattrs_setup(&qwork_ed.xattrs);
        if (in->process_xattrs) {
            xattrs_get(qwork->name, &qwork_ed.xattrs);
        }

        /*
         * there is no work to do for a directory here - we are
         * processing files and links of this dir into a gufi db
         */
        if (!S_ISDIR(qwork->statuso.st_mode)) {
            if (S_ISLNK(qwork->statuso.st_mode)) {
                qwork_ed.type = 'l';
                readlink(qwork->name, qwork_ed.linkname, MAXPATH);
            } else if (S_ISREG(qwork->statuso.st_mode)) {
                qwork_ed.type = 'f';
            }

            sumit(&summary, qwork, &qwork_ed);
            insertdbgo(qwork, &qwork_ed, res);
        }

        xattrs_cleanup(&qwork_ed.xattrs);
        free(qwork);
    }

    stopdb(db);
    insertdbfin(res);

    /* this i believe has to be after we close off the entries transaction */
    insertsumdb(db, passmywork->name, passmywork, &ed, &summary);
    xattrs_cleanup(&ed.xattrs);
    closedb(db);

    chown(dbpath, passmywork->statuso.st_uid, passmywork->statuso.st_gid);
    chmod(dbpath, (passmywork->statuso.st_mode & ~(S_IXUSR | S_IXGRP | S_IXOTH)) | S_IRUSR);

    return 0;
}

static int stripe_index(struct input *in, const ino_t inode, const int id) {
    if (in->stride > 0) {
        return (inode / in->stride) % in->maxthreads; /* striping inodes */
    }
    return id;
}

static int processdir(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct work *passmywork = data;
    struct PoolArgs *pa = (struct PoolArgs *) args;
    struct input *in = &pa->in;

    DIR *dir = opendir(passmywork->name);
    if (!dir) {
        const int err = errno;
        fprintf(stderr, "couldn't open dir '%s': %s\n",
                passmywork->name, strerror(err));
        goto out_free;
    }

    struct entry_data ed;
    memset(&ed, 0, sizeof(ed));
    if (lstat_wrapper(passmywork) != 0) {
        const int err = errno;
        fprintf(stderr, "couldn't stat dir '%s': %s\n",
                passmywork->name, strerror(err));
        goto out_dir;
    }

    ed.type = 'd';
    ed.suspect = in->suspectd;

    /*
     * if we are putting the gufi index into the source tree we can
     * modify the suspecttime to be the mtime of the gufi db
     *
     * this way we will just be looking at dirs or files that have
     * changed since the gufi db was last updated
     */
    int locsuspecttime = in->suspecttime;
    if (in->buildindex == 1) {
        locsuspecttime = 0; /* all timestamps are suspect */
        char dbpath[MAXPATH];
        SNPRINTF(dbpath, MAXPATH, "%s/%s", passmywork->name, DBNAME);
        struct stat st;
        if (lstat(dbpath, &st) == 0) {
            locsuspecttime = st.st_mtime;
        } else {
            ed.suspect = 1;
        }
    }

    /*
     * if we are not looking for suspect directories we should
     * just put the directory at the top of all the dirents
     */
    if (in->output == OUTDB) {
        if (in->insertdir > 0) {
            if (in->suspectmethod == 0) {
                const int todb = stripe_index(in, passmywork->statuso.st_ino, id);
                struct ThreadArgs *ta = &pa->ta[todb];

                if (in->stride > 0) {
                    pthread_mutex_lock(&ta->mutex);
                }

                startdb(ta->db);
                insertdbgor(passmywork, &ed, ta->res);

                if (in->stride > 0) {
                    stopdb(ta->db);
                    pthread_mutex_unlock(&ta->mutex);
                }
            }
            else if (in->suspectmethod == 1) {  /* look up inode in trie to see if this is a suspect dir */
                ed.suspect = searchmyll(pa, passmywork->statuso.st_ino, DFL_DIR); /* set the directory suspect flag on so we will mark it in output */
            }
            else if (in->suspectmethod > 1) {
                /* mark the dir suspect if mtime or ctime are >= provided last run time */
                lstat_wrapper(passmywork);
                if (passmywork->statuso.st_ctime >= locsuspecttime) ed.suspect = 1;
                if (passmywork->statuso.st_mtime >= locsuspecttime) ed.suspect = 1;
            }
        }
    }
    else if (in->output == OUTFILE) {
        const int tooutfile = stripe_index(in, passmywork->statuso.st_ino, id);
        struct ThreadArgs *ta = &pa->ta[tooutfile];

        if (in->stride > 0) {
            pthread_mutex_lock(&ta->mutex);
        }

        /* only directories are here so sortf is set to the directory full pathname */
        fprintf(ta->file, "%s%c%" STAT_ino "%c%lld%c%c%c%s%c\n",
                passmywork->name,            in->delim,
                passmywork->statuso.st_ino,  in->delim,
                passmywork->pinode,          in->delim,
                ed.type,                     in->delim,
                passmywork->name,            in->delim);

        if (in->stride > 0) {
            pthread_mutex_unlock(&ta->mutex);
        }
    }

    /*
     * loop over dirents, if dir push it on the queue, if file or link
     * print it, fill up qwork structure for each
     */
    struct dirent *entry = NULL;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        if (((len == 1) && !strncmp(entry->d_name, ".",  1)) ||
            ((len == 2) && !strncmp(entry->d_name, "..", 2)))
            continue;

        if (in->buildindex == 1) {
            if ((len == DBNAME_LEN) && !strncmp(entry->d_name, DBNAME, DBNAME_LEN))
                continue;
        }

        struct work *qwork = new_work_with_name(passmywork->name, passmywork->name_len, entry->d_name, len);
        struct entry_data qwork_ed;
        memset(&qwork_ed, 0, sizeof(qwork_ed));

        try_skip_lstat(entry->d_type, qwork);

        qwork->pinode = passmywork->statuso.st_ino;
        qwork->statuso.st_ino = entry->d_ino;

        if (entry->d_type == DT_DIR) {
            QPTPool_enqueue(ctx, id, processdir, qwork);
            continue;
        }
        else if (entry->d_type == DT_LNK) {
            qwork_ed.type = 'l';
        }
        else if (entry->d_type == DT_REG) {
            qwork_ed.type = 'f';
        }

        /*
         * if suspect method is not zero then we can insert files and links
         * if not we dont care about files and links in db
         */
        if (in->suspectmethod) {
            if (in->output == OUTDB) {
                if (in->insertfl > 0) {
                    const int todb = stripe_index(in, passmywork->statuso.st_ino, id);
                    struct ThreadArgs *ta = &pa->ta[todb];

                    if (in->stride > 0) {
                        pthread_mutex_lock(&ta->mutex);
                        startdb(ta->db);
                    }

                    insertdbgor(qwork, &qwork_ed, ta->res);

                    if (in->stride > 0) {
                        stopdb(ta->db);
                        pthread_mutex_unlock(&ta->mutex);
                    }
                }
            }

            if (ed.suspect == 0) {                /* if suspect dir just skip looking any further */
                if ((in->suspectmethod == 1) ||
                    (in->suspectmethod == 2)) {   /* if method 1 or 2 we look up the inode in trie and mark dir suspect or not */
                    ed.suspect = searchmyll(pa, qwork->statuso.st_ino, DFL_FL); /* set the directory suspect flag on so we will mark it in output */
                }
                if (in->suspectmethod > 2) {
                    /* stat the file/link and if ctime or mtime is >= provided last run time mark dir suspect */
                    lstat_wrapper(qwork);
                    if (qwork->statuso.st_ctime >= locsuspecttime) ed.suspect = 1;
                    if (qwork->statuso.st_mtime >= locsuspecttime) ed.suspect = 1;
                }
            }

            if (in->output == OUTFILE) {
                const int tooutfile = stripe_index(in, passmywork->statuso.st_ino, id);
                struct ThreadArgs *ta = &pa->ta[tooutfile];

                if (in->stride > 0) {
                    pthread_mutex_lock(&ta->mutex);
                }

                /*
                 * since this is a file or link, we need the path to
                 * the file or link without the name as the sortf
                 */
                fprintf(ta->file, "%s%c%" STAT_ino "%c%lld%c%c%c%s%c\n",
                        qwork->name,           in->delim,
                        qwork->statuso.st_ino, in->delim,
                        qwork->pinode,         in->delim,
                        qwork_ed.type,         in->delim,
                        passmywork->name,      in->delim);

                if (in->stride > 0) {
                    pthread_mutex_unlock(&ta->mutex);
                }
            }
        }

        free(qwork);
    }

    /*
     * if we are not looking for suspect directories we should just
     * put the directory at the top of all the dirents
     */
    if (in->suspectmethod > 0) {
        if (in->output == OUTDB) {
            if (in->insertdir > 0) {
                const int todb = stripe_index(in, passmywork->statuso.st_ino, id);
                struct ThreadArgs *ta = &pa->ta[todb];

                if (in->stride > 0) {
                    pthread_mutex_lock(&ta->mutex);
                }

                startdb(ta->db);
                insertdbgor(passmywork, &ed, ta->res);

                if (in->stride > 0) {
                    stopdb(ta->db);
                    pthread_mutex_unlock(&ta->mutex);
                }
            }
        }
    }

    if (in->output == OUTDB) {
        if (in->insertdir > 0) {
            if (in->stride == 0) {
                stopdb(pa->ta[id].db);
            }
        }
    }

    if (ed.suspect == 1) {
        if (pa->gltodirmode == 1) {
            /* we may not have stat on the directory we may be told its suspect somehow not stating it */
            if (reprocessdir(in, passmywork, dir)) {
                fprintf(stderr, "problem producing gufi db for suspect directory\n");
            }
        }
    }

  out_dir:
    closedir(dir);

  out_free:
    free(passmywork);

    return 0;
}

static int processinit(struct PoolArgs *pa, QPTPool_t *ctx) {
    if (pa->in.suspectfile > 0) {
        FILE *isf = fopen(pa->in.insuspect.data, "r");
        if(!isf) {
            fprintf(stderr, "Cant open input suspect file %s\n", pa->in.insuspect.data);
            return 1;
        }

        long long int cntfl = 0;
        long long int cntd = 0;

        /* set up triell for directories and one for files and links */
        pa->dirs = trie_alloc();
        pa->fls = trie_alloc();

        char incsuspect[24];
        char incsuspecttype;
        while (fscanf(isf,"%s %c", incsuspect, &incsuspecttype) != EOF) {
            ino_t testll = 0;
            if (sscanf(incsuspect, "%" STAT_ino, &testll) != 1) {
                continue;
            }

            switch (incsuspecttype) {
                case 'd':
                    if (cntd==0) {
                        pa->glsuspectdmin = testll;
                        pa->glsuspectdmax = testll;
                    } else {
                        if (testll < pa->glsuspectdmin) pa->glsuspectdmin = testll;
                        if (testll > pa->glsuspectdmax) pa->glsuspectdmax = testll;
                    }

                    trie_insert(pa->dirs, incsuspect, strlen(incsuspect), NULL, NULL);
                    cntd++;
                    break;
                case 'f': case 'l':
                    if (cntfl==0) {
                        pa->glsuspectflmin = testll;
                        pa->glsuspectflmax = testll;
                    } else {
                        if (testll < pa->glsuspectflmin) pa->glsuspectflmin = testll;
                        if (testll > pa->glsuspectflmax) pa->glsuspectflmax = testll;
                    }

                    trie_insert(pa->fls, incsuspect, strlen(incsuspect), NULL, NULL);
                    cntfl++;
                    break;
            }
        }

        fclose(isf);
    }

    pa->ta = calloc(pa->in.maxthreads, sizeof(struct ThreadArgs));

    if (pa->in.output == OUTDB) {
        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            struct ThreadArgs *ta = &pa->ta[i];

            char outname[MAXPATH];
            SNPRINTF(outname, MAXPATH, "%s.%zu", pa->in.outname.data, i);

            ta->db = opendb(outname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                            1, 1, create_readdirplus_tables, NULL);
            ta->res = insertdbprep(ta->db, READDIRPLUS_INSERT);
        }
    }
    else if (pa->in.output == OUTFILE) {
        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            struct ThreadArgs *ta = &pa->ta[i];

            char outname[MAXPATH];
            SNPRINTF(outname, MAXPATH, "%s.%zu", pa->in.outname.data, i);

            ta->file = fopen(outname, "w");
        }
    }

    if (pa->in.stride > 0) {
        for(size_t i = 0; i < pa->in.maxthreads; i++) {
            struct ThreadArgs *ta = &pa->ta[i];
            pthread_mutex_init(&ta->mutex, NULL);
        }
    }

    struct work *mywork = new_work_with_name(NULL, 0, pa->in.name.data, pa->in.name.len);

    QPTPool_enqueue(ctx, 0, processdir, mywork);

    return 0;
}

static int processfin(struct PoolArgs *pa) {
    for(size_t i = 0; i < pa->in.maxthreads; i++) {
        struct ThreadArgs *ta = &pa->ta[i];
        if (ta->file) {
            fclose(ta->file);
        }

        insertdbfin(ta->res);
        closedb(ta->db);

        if (pa->in.stride > 0) {
            pthread_mutex_destroy(&ta->mutex);
        }
    }

    trie_free(pa->fls);
    trie_free(pa->dirs);
    free(pa->ta);
    input_fini(&pa->in);

    return 0;
}

static int validate_inputs(struct input *in) {
    if (in->buildindex) {
        fprintf(stderr, "You are putting the index dbs in input directory\n");
        in->nameto = in->name;
    }

    return 0;
}

static void sub_help(void) {
    printf("input_dir                walk this tree to produce GUFI index\n");
    printf("index                    build GUFI index here\n");
    printf("\n");
}

int main(int argc, char *argv[]) {
    /* process input args - all programs share the common 'struct input', */
    /* but allow different fields to be filled at the command-line. */
    /* Callers provide the options-string for get_opt(), which will */
    /* control which options are parsed for each program. */
    struct PoolArgs pa;
    memset(&pa, 0, sizeof(pa));
    int idx = parse_cmd_line(argc, argv, "hHvn:O:o:d:rRYZW:g:A:c:xb", 1, "input_dir [index]", &pa.in);
    if (pa.in.helped)
        sub_help();

    if (idx < 0) {
        input_fini(&pa.in);
        return -!pa.in.helped;
    }
    else {
        INSTALL_STR(&pa.in.name, argv[idx++]);

        if (idx < argc) {
            INSTALL_STR(&pa.in.nameto, argv[idx++]);
        }
    }

    if (validate_inputs(&pa.in)) {
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    /* check the output directory for the gufi dbs for suspect dirs if provided */
    pa.gltodirmode = 0;
    if (pa.in.nameto.len > 0) {
        pa.gltodirmode = 1;
        /* make sure the directory to put the gufi dbs into exists and we can write to it */
        struct stat st;
        if (lstat(pa.in.nameto.data, &st) != 0) {
            fprintf(stdout, "directory to place gufi dbs problem for %s\n", pa.in.nameto.data);
            input_fini(&pa.in);
            return EXIT_FAILURE;
        }
        if (!S_ISDIR(st.st_mode)) {
            fprintf(stdout, "directory to place gufi dbs is not a directory\n");
            input_fini(&pa.in);
            return EXIT_FAILURE;
        }
    }

    if (pa.in.buildindex) {
        pa.gltodirmode = 1;
    }

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        input_fini(&pa.in);
        return EXIT_FAILURE;
    }

    processinit(&pa, pool);

    QPTPool_stop(pool);

    QPTPool_destroy(pool);

    processfin(&pa);

    return EXIT_SUCCESS;
}
