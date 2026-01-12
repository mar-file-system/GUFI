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

/*
 * To build:
 *
 *     export LUSTRE_LIBRARY_DIR=/home/$USER/git/lustre-release/lustre/utils/.libs
 *     export LUSTRE_INCLUDE_DIR=/home/$USER/git/lustre-release/lustre/include/
 *     gcc -D_GNU_SOURCE -I$LUSTRE_INCLUDE_DIR -I$LUSTRE_INCLUDE_DIR/uapi/ -L$LUSTRE_LIBRARY_DIR -llustreapi changelog_reader.c
 *
 * To run:
 *
 *     export LD_LIBRARY_PATH=$LUSTRE_LIBRARY_DIR
 *     ./a.out
 *
 */

#include <stdio.h>
#include <getopt.h>

#include "lustre/lustreapi.h"

/** returns fid object sequence */
static inline __u64 fid_seq(const struct lu_fid *fid)
{
	return fid->f_seq;
}

/** returns fid object id */
static inline __u32 fid_oid(const struct lu_fid *fid)
{
	return fid->f_oid;
}

/**
 * Flatten 128-bit FID values into a 64-bit value for use as an inode number.
 *
 * This is taken from lustre/include/uapi/linux/lustre/lustre_fid.h, with
 * irrelevant parts (i.e., IGIF FIDs) removed.
 *
 * It would be better to directly include that header, but it's not part of the
 * public Lustre API, and is also not designed for inclusion in userspace programs
 * (e.g., it includes kernel headers which cannot be transitively included in
 * userspace programs).
 *
 * So for lack of a better option, I copy the definition.
 */
static inline __u64 fid_flatten64(const struct lu_fid *fid)
{
	__u64 ino;
	__u64 seq;

	seq = fid_seq(fid);

	ino = (seq << 24) + ((seq >> 24) & 0xffffff0000ULL) + fid_oid(fid);

	return ino ?: fid_oid(fid);
}

static struct {
    char *consumer_name;    /* e.g., 'cl1' */
    char *mdt_name;         /* e.g., 'lustre-MDT0000' */
    bool keep_records;      /* whether to clear the records that were consumed */
} args;

int parse_args(int argc, char *argv[])
{
    int opt;

    args.keep_records = false;

    static struct option long_options[] = {
        {"keep", no_argument, 0, 'k'},
        {"mdt",      required_argument, 0, 'm'},
        {"consumer_id", required_argument, 0, 'i'},
        {"help",     no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    while ((opt = getopt_long(argc, argv, "hi:km:", long_options, NULL)) != -1) {
        switch (opt) {
            case 'i':
                args.consumer_name = optarg;
                break;
            case 'k':
                args.keep_records = true;
                break;
            case 'm':
                args.mdt_name = optarg;
                break;
            case 'h':
                goto usage;
            default:
                goto usage;
        }
    }

    // printf("%s %s %d\n", args.mdt_name, args.consumer_name, args.clear);

    if (!args.mdt_name || !args.consumer_name) {
        goto usage;
    }

    return 0;

usage:
    printf("Usage: %s --mdt <mdt> --consumer_id <consumer> [--keep]\n", argv[0]);
    return 1;
}

int main(int argc, char *argv[])
{
    void *changelog_priv;
    long long startrec = 0;
    struct changelog_rec *rec;
    int rc;

    if (parse_args(argc, argv)) {
        return EXIT_FAILURE;
    }

	rc = llapi_changelog_start(&changelog_priv,
				   CHANGELOG_FLAG_JOBID |
				   CHANGELOG_FLAG_EXTRA_FLAGS,
				   args.mdt_name, startrec);
    if (rc < 0) {
        fprintf(stderr, "error starting changelog: %s\n", strerror(-rc));
        return EXIT_FAILURE;
    }

    __u64 index;
    while ((rc = llapi_changelog_recv(changelog_priv, &rec)) == 0) {
        index = rec->cr_index;

        __u32 type = rec->cr_type;

        /* Filter out record types that we aren't interested in: */
        if (type != CL_CREATE &&
            type != CL_MKDIR &&
            type != CL_HARDLINK &&
            type != CL_SOFTLINK &&
            type != CL_MKNOD &&
            type != CL_UNLINK &&
            type != CL_RMDIR &&
            type != CL_RENAME &&
            type != CL_SETATTR &&
            type != CL_SETXATTR &&
            type != CL_MTIME ) {
            continue;
        }

        /*
        printf("record: %s, fid: "DFID", parent: "DFID,
                changelog_type2str(rec->cr_type),
                PFID(&rec->cr_tfid),
                PFID(&rec->cr_pfid));
        printf(" inode: ");
        */

        printf("%llu\n", fid_flatten64(&rec->cr_pfid));
    }

    if (!args.keep_records) {
        llapi_changelog_clear(args.mdt_name, args.consumer_name, index);
    }

    llapi_changelog_fini(&changelog_priv);

    return rc == 1 ? EXIT_SUCCESS : EXIT_FAILURE;
}
