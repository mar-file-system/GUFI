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
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"
#include "dbutils.h"
#include "template_db.h"
#include "utils.h"

extern int errno;

// constants set at runtime (probably cannot be constexpr)
int templatefd = -1;
off_t templatesize = 0;

#if BENCHMARK
#include <time.h>

pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
size_t total_dirs = 0;
size_t total_files = 0;
#endif

int processdir(struct QPTPool * ctx, const size_t id, void * data, void * args) {
    #if BENCHMARK
    pthread_mutex_lock(&global_mutex);
    total_dirs++;
    pthread_mutex_unlock(&global_mutex);
    #endif

    /* skip argument checking */
    /* if (!data) { */
    /*     return 1; */
    /* } */

    /* if (!ctx || (id >= ctx->size)) { */
    /*     free(data); */
    /*     return 1; */
    /* } */

    struct work * work = (struct work *) data;

    DIR * dir = opendir(work->name);
    if (!dir) {
        fprintf(stderr, "Could not open directory \"%s\"\n", work->name);
        return 1;
    }

    // get source directory info
    struct stat dir_st;
    if (lstat(work->name, &dir_st) < 0)  {
        closedir(dir);
        return 1;
    }

    // create the directory
    char topath[MAXPATH];
    SNPRINTF(topath, MAXPATH, "%s/%s", in.nameto, work->name + in.name_len); /* offset by in.name_len to remove prefix */
    int rc = mkdir(topath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);           /* don't need recursion because parent is guaranteed to exist */
    if (rc < 0) {
        const int err = errno;
        if (err != EEXIST) {
            fprintf(stderr, "mkdir %s failure: %d %s\n", topath, err, strerror(err));
            closedir(dir);
            return 1;
        }
    }

    // create the database name
    char dbname[MAXPATH];
    SNPRINTF(dbname, MAXPATH, "%s/" DBNAME, topath);

    // copy the template file
    if (copy_template(templatefd, dbname, templatesize, dir_st.st_uid, dir_st.st_gid)) {
        closedir(dir);
        return 1;
    }

    sqlite3 * db = opendb(dbname, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, 1, 0
                          , NULL, NULL
                          #if defined(DEBUG) && defined(PER_THREAD_STATS)
                          , NULL, NULL
                          , NULL, NULL
                          #endif
                          );
    if (!db) {
        closedir(dir);
        return 1;
    }

    // prepare to insert into the database
    struct sum summary;
    zeroit(&summary);

    sqlite3_stmt * res = insertdbprep(db);

    startdb(db);

    struct dirent * entry = NULL;
    size_t rows = 0;
    while ((entry = readdir(dir))) {
        const size_t len = strlen(entry->d_name);

        // skip . and ..
        if (entry->d_name[0] == '.') {
            if ((len == 1) ||
                ((len == 2) && (entry->d_name[1] == '.'))) {
                continue;
            }
        }


        // get entry path
        struct work e;
        memset(&e, 0, sizeof(struct work));
        SNFORMAT_S(e.name, MAXPATH, 3, work->name, strlen(work->name), "/", 1, entry->d_name, len);

        // get the entry's metadata
        if (lstat(e.name, &e.statuso) < 0) {
            continue;
        }

        /* e.xattrs_len = 0; */
        if (in.doxattrs > 0) {
            e.xattrs_len = pullxattrs(e.name, e.xattrs, sizeof(e.xattrs));
        }

        // push subdirectories onto the queue
        if (S_ISDIR(e.statuso.st_mode)) {
            if (work->level < in.max_level) {
                e.type[0] = 'd';
                e.pinode = work->statuso.st_ino;
                e.level = work->level + 1;

                /* make a copy here so that the data can be pushed into the queue */
                /* this is more efficient than malloc+free for every single entry */
                struct work * copy = (struct work *) calloc(1, sizeof(struct work));
                memcpy(copy, &e, sizeof(struct work));

                QPTPool_enqueue(ctx, id, processdir, copy);
                continue;
            }
        }

        rows++;

        // non directories
        if (S_ISLNK(e.statuso.st_mode)) {
            e.type[0] = 'l';
            readlink(e.name, e.linkname, MAXPATH);
        }
        else if (S_ISREG(e.statuso.st_mode)) {
            e.type[0] = 'f';
        }
        else {
            /* other types are not stored */
            continue;
        }

        #if BENCHMARK
        pthread_mutex_lock(&global_mutex);
        total_files++;
        pthread_mutex_unlock(&global_mutex);
        #endif

        // get entry relative path
        char e_name[MAXPATH];
        SNPRINTF(e_name, MAXPATH, "%s", e.name + in.name_len);

        // overwrite full path with relative path
        SNFORMAT_S(e.name, MAXPATH, 1, e_name, strlen(e.name) - in.name_len);

        // update summary table
        sumit(&summary, &e);

        // add entry into bulk insert
        insertdbgo(&e, db, res);
    }

    stopdb(db);
    insertdbfin(res);
    insertsumdb(db, work, &summary);
    closedb(db);
    db = NULL;

    // ignore errors
    chmod(topath, work->statuso.st_mode);
    chown(topath, work->statuso.st_uid, work->statuso.st_gid);

    closedir(dir);

    free(work);

    return 0;
}

struct work * validate_inputs() {
    char expathin[MAXPATH];
    char expathout[MAXPATH];
    char expathtst[MAXPATH];

    SNPRINTF(expathtst,MAXPATH,"%s/%s",in.nameto,in.name);
    realpath(expathtst,expathout);
    //printf("expathtst: %s expathout %s\n",expathtst,expathout);
    realpath(in.name,expathin);
    //printf("in.name: %s expathin %s\n",in.name,expathin);
    if (!strcmp(expathin,expathout)) {
        fprintf(stderr,"You are putting the index dbs in input directory\n");
    }

    struct work * root = (struct work *) calloc(1, sizeof(struct work));
    if (!root) {
        fprintf(stderr, "Could not allocate root struct\n");
        return NULL;
    }

    SNPRINTF(root->name, MAXPATH, "%s", in.name);

    // get input path metadata
    if (lstat(root->name, &root->statuso) < 0) {
        fprintf(stderr, "Could not stat source directory \"%s\"\n", in.name);
        free(root);
        return NULL;
    }

    // check that the input path is a directory
    if (S_ISDIR(root->statuso.st_mode)) {
        root->type[0] = 'd';
    }
    else {
        fprintf(stderr, "Source path is not a directory \"%s\"\n", in.name);
        free(root);
        return NULL;
    }

    // check if the source directory can be accessed
    if (access(root->name, R_OK | X_OK) != 0) {
        fprintf(stderr, "couldn't access input dir '%s': %s\n",
                root->name, strerror(errno));
        free(root);
        return NULL;
    }

    if (!strlen(in.nameto)) {
        fprintf(stderr, "No output path specified\n");
        free(root);
        return NULL;
    }

    // check if the destination path already exists (not an error)
    struct stat dst_st;
    if (lstat(in.nameto, &dst_st) == 0) {
        fprintf(stderr, "\"%s\" Already exists!\n", in.nameto);

        // if the destination path is not a directory (error)
        if (!S_ISDIR(dst_st.st_mode)) {
            fprintf(stderr, "Destination path is not a directory \"%s\"\n", in.nameto);
            free(root);
            return NULL;
        }
    }

    // check the output files, if a prefix was provided
    if (in.outfile) {
        if (!strlen(in.outfilen)) {
            fprintf(stderr, "No output file name specified\n");
            free(root);
            return NULL;
        }

        // check if the destination path already exists (not an error)
        for(int i = 0; i < in.maxthreads; i++) {
            char outname[MAXPATH];
            SNPRINTF(outname, MAXPATH, "%s.%d", in.outfilen, i);

            struct stat dst_st;
            if (lstat(in.outfilen, &dst_st) == 0) {
                fprintf(stderr, "\"%s\" Already exists!\n", in.nameto);

                // if the destination path is not a directory (error)
                if (S_ISDIR(dst_st.st_mode)) {
                    fprintf(stderr, "Destination path is a directory \"%s\"\n", in.outfilen);
                    free(root);
                    return NULL;
                }
            }
        }
    }

    // check the output dbs, if a prefix was provided
    if (in.outdb) {
        if (!strlen(in.outdbn)) {
            fprintf(stderr, "No output db name specified\n");
            free(root);
            return NULL;
        }

        // check if the destination path already exists (not an error)
        for(int i = 0; i < in.maxthreads; i++) {
            char outname[MAXPATH];
            SNPRINTF(outname, MAXPATH, "%s.%d", in.outdbn, i);

            struct stat dst_st;
            if (lstat(in.outdbn, &dst_st) == 0) {
                fprintf(stderr, "\"%s\" Already exists!\n", in.nameto);

                // if the destination path is not a directory (error)
                if (S_ISDIR(dst_st.st_mode)) {
                    fprintf(stderr, "Destination path is a directory \"%s\"\n", in.outdbn);
                    free(root);
                    return NULL;
                }
            }
        }
    }

    // create the source root under the destination directory using
    // the source directory's permissions and owners
    // this allows for the threads to not have to recursively create directories
    char dst_path[MAXPATH];
    SNPRINTF(dst_path, MAXPATH, "%s/%s", in.nameto, in.name + in.name_len);
    if (dupdir(dst_path, &root->statuso)) {
        fprintf(stderr, "Could not create %s under %s\n", in.name, in.nameto);
        free(root);
        return NULL;
    }

    if (in.doxattrs > 0) {
        root->xattrs_len = pullxattrs(in.name, root->xattrs, sizeof(root->xattrs));
    }

    root->level = 0;

    return root;
}

void sub_help() {
   printf("input_dir         walk this tree to produce GUFI index\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

int main(int argc, char * argv[]) {
    int idx = parse_cmd_line(argc, argv, "hHn:xz:", 2, "input_dir output_dir", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_dir");
        INSTALL_STR(in.nameto, argv[idx++], MAXPATH, "output_dir");

        if (retval)
            return retval;

        in.name_len = strlen(in.name);
        remove_trailing(in.name, &in.name_len, "/", 1);

        /* root is special case */
        if (in.name_len == 0) {
            in.name[0] = '/';
            in.name_len = 1;
        }
        else {
            /* add 1 more for the separator that is placed between this string and the entry */
            in.name_len++;
        }
    }

    // get first work item by validating inputs
    struct work * root = validate_inputs();
    if (!root) {
        return -1;
    }

    #if BENCHMARK
    fprintf(stderr, "Creating GUFI Index %s in %s with %d threads\n", in.name, in.nameto, in.maxthreads);
    #endif

    if ((templatesize = create_template(&templatefd)) == (off_t) -1) {
        fprintf(stderr, "Could not create template file\n");
        return -1;
    }

    #if BENCHMARK
    struct start_end benchmark;
    clock_gettime(CLOCK_MONOTONIC, &benchmark.start);
    #endif

    struct QPTPool * pool = QPTPool_init(in.maxthreads
                                         #if defined(DEBUG) && defined(PER_THREAD_STATS)
                                         , NULL
                                         #endif
        );
    if (!pool) {
        fprintf(stderr, "Failed to initialize thread pool\n");
        return -1;
    }

    if (QPTPool_start(pool, NULL) != (size_t) in.maxthreads) {
        fprintf(stderr, "Failed to start threads\n");
        return -1;
    }

    QPTPool_enqueue(pool, 0, processdir, root);
    QPTPool_wait(pool);
    QPTPool_destroy(pool);

    #if BENCHMARK
    clock_gettime(CLOCK_MONOTONIC, &benchmark.end);
    const long double processtime = sec(elapsed(&benchmark));

    fprintf(stderr, "Total Dirs:            %zu\n",    total_dirs);
    fprintf(stderr, "Total Files:           %zu\n",    total_files);
    fprintf(stderr, "Time Spent Indexing:   %.2Lfs\n", processtime);
    fprintf(stderr, "Dirs/Sec:              %.2Lf\n",  total_dirs / processtime);
    fprintf(stderr, "Files/Sec:             %.2Lf\n",  total_files / processtime);
    #endif

    close(templatefd);

    return 0;
}
