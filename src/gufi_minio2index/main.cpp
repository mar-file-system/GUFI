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



#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "miniocpp/client.h"

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "debug.h"
#include "utils.h"

#include "gufi_minio2index/policies.h"
#include "gufi_minio2index/process.h"
#include "gufi_minio2index/structs.h"

/*
 * create the target directory
 *
 * note that the provided directories go into
 * individual directories underneath this one
 */
static int setup_dst(const char *nameto) {
    /* check if the destination path already exists (not an error) */
    struct stat dst_st;
    if (lstat(nameto, &dst_st) == 0) {
        fprintf(stderr, "\"%s\" Already exists!\n", nameto);

        /* if the destination path is not a directory (error) */
        if (!S_ISDIR(dst_st.st_mode)) {
            fprintf(stderr, "Destination path is not a directory \"%s\"\n", nameto);
            return -1;
        }

        return 0;
    }

    struct stat st;
    st.st_mode = S_IRWXU | S_IRWXG | S_IRWXO;
    st.st_uid = geteuid();
    st.st_gid = getegid();

    if (dupdir(nameto, &st)) {
        fprintf(stderr, "Could not create %s\n", nameto);
        return -1;
    }

    return 0;
}

void sub_help() {
   printf("server            MinIO server URL\n");
   printf("user              account to log in as\n");
   printf("password          password for account\n");
   printf("input_dir...      walk one or more trees to produce GUFI index\n");
   printf("output_dir        build GUFI index here\n");
   printf("\n");
}

int main(int argc, char *argv[]) {
    struct PoolArgs pa;

    refstr_t server;
    refstr_t user;
    refstr_t password;

    int idx = parse_cmd_line(argc, argv, "hHn:z:k:" COMPRESS_OPT, 5, "server user password input_dir... output_dir", &pa.in);
    if (pa.in.helped)
        sub_help();
    if (idx < 0)
        return -1;
    else {
        /* parse positional args, following the options */
        int retval = 0;

        INSTALL_STR(&server, argv[idx++]);
        INSTALL_STR(&user, argv[idx++]);
        INSTALL_STR(&password, argv[idx++]);

        /* does not have to be canonicalized */
        INSTALL_STR(&pa.in.nameto, argv[argc - 1]);

        if (retval)
            return retval;
    }

    if (setup_directory_skip(pa.in.skip.data, &pa.skip) != 0) {
        return -1;
    }

    if (setup_dst(pa.in.nameto.data) != 0) {
        trie_free(pa.skip);
        return -1;
    }

    init_template_db(&pa.policies);
    if (create_policies_template(&pa.policies) != 0) {
        fprintf(stderr, "Could not create policies template file\n");
        trie_free(pa.skip);
        return -1;
    }

    init_template_db(&pa.db);
    if (create_dbdb_template(&pa.db) != 0) {
        fprintf(stderr, "Could not create template file\n");
        close_template_db(&pa.policies);
        trie_free(pa.skip);
        return -1;
    }

    init_template_db(&pa.xattr);
    if (create_xattrs_template(&pa.xattr) != 0) {
        fprintf(stderr, "Could not create xattr template file\n");
        close_template_db(&pa.db);
        close_template_db(&pa.policies);
        trie_free(pa.skip);
        return -1;
    }

    minio::s3::BaseUrl base_url(server.data);
    base_url.https = false;

    minio::creds::StaticProvider provider(user.data, password.data);
    minio::s3::Client client(base_url, &provider);

    QPTPool_t *pool = QPTPool_init(pa.in.maxthreads, &pa);
    if (!pool) {
        fprintf(stderr, "Error: Failed to initialize thread pool\n");
        close_template_db(&pa.xattr);
        close_template_db(&pa.db);
        close_template_db(&pa.policies);
        trie_free(pa.skip);
        return -1;
    }

    if (QPTPool_start(pool) != 0) {
        fprintf(stderr, "Error: Failed to start thread pool\n");
        QPTPool_destroy(pool);
        close_template_db(&pa.xattr);
        close_template_db(&pa.db);
        close_template_db(&pa.policies);
        trie_free(pa.skip);
        return -1;
    }

    fprintf(stdout, "Creating GUFI Index %s with %zu threads\n", pa.in.nameto.data, pa.in.maxthreads);

    pa.client = &client;
    pa.all_buckets.resize(pa.in.maxthreads);
    pa.total.resize(pa.in.maxthreads);

    struct start_end after_init;
    clock_gettime(CLOCK_MONOTONIC, &after_init.start);

    const size_t root_count = argc - idx - 1;
    char **roots = (char **) calloc(root_count, sizeof(char *));
    for(size_t i = 0; idx < (argc - 1);) {
        /* force all input paths to be canonical */
        roots[i] = realpath(argv[idx++], NULL);
        if (!roots[i]) {
            const int err = errno;
            fprintf(stderr, "Could not resolve path \"%s\": %s (%d)\n",
                    argv[idx - 1], strerror(err), err);
            continue;
        }

        QPTPool_enqueue(pool, 0, processroot, roots[i]);
        i++;
    }

    QPTPool_wait(pool);

    clock_gettime(CLOCK_MONOTONIC, &after_init.end);
    const long double processtime = sec(nsec(&after_init));

    /* don't count as part of processtime */
    QPTPool_destroy(pool);

    for(size_t i = 0; i < root_count; i++) {
        free(roots[i]);
    }
    free(roots);
    close_template_db(&pa.xattr);
    close_template_db(&pa.db);
    close_template_db(&pa.policies);
    trie_free(pa.skip);

    uint64_t total_buckets = 0;
    uint64_t total_subbuckets = 0;
    uint64_t total_objects = 0;
    for(size_t i = 0; i < pa.in.maxthreads; i++) {
        total_buckets    += pa.total[i].buckets;
        total_subbuckets += pa.total[i].subbuckets;
        total_objects    += pa.total[i].objects;
    }

    fprintf(stdout, "Total Buckets:       %" PRIu64 "\n", total_buckets);
    fprintf(stdout, "Total Subbuckets:    %" PRIu64 "\n", total_subbuckets);
    fprintf(stdout, "Total Objects:       %" PRIu64 "\n", total_objects);
    fprintf(stdout, "Time Spent Indexing: %.2Lfs\n",      processtime);
    fprintf(stdout, "Objects/Sec:         %.2Lf\n",       total_objects / processtime);

    return 0;
}
