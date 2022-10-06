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
#include <string.h>
#include <stdlib.h>

#include "utils.h"
#include "xattrs.h"

const char XATTRDELIM[] = "\x1F";     // ASCII Unit Separator

const char XATTRS_PWD_CREATE[] = "DROP TABLE IF EXISTS " XATTRS_PWD ";"
                                 "CREATE TABLE " XATTRS_PWD "(inode INT64, name TEXT, value TEXT);";

const char XATTRS_PWD_INSERT[] = "INSERT INTO " XATTRS_PWD " VALUES (@inode, @name, @value);";

const char XATTRS_ROLLUP_CREATE[] = "DROP TABLE IF EXISTS " XATTRS_ROLLUP ";"
                                    "CREATE TABLE " XATTRS_ROLLUP "(inode INT64, name TEXT, value TEXT);";

const char XATTRS_ROLLUP_INSERT[] = "INSERT INTO " XATTRS_ROLLUP " VALUES (@inode, @name, @value);";

const char XATTRS_AVAIL_CREATE[] = "DROP VIEW IF EXISTS " XATTRS_AVAIL ";"
                                   "CREATE VIEW " XATTRS_AVAIL " AS SELECT * FROM " XATTRS_PWD " UNION SELECT * FROM " XATTRS_ROLLUP ";";

const char XATTR_UID_FILENAME_FORMAT[]         = "uid.%llu.db";
const char XATTR_UID_ATTACH_FORMAT[]           = "uid%llu";
const char XATTR_GID_W_READ_FILENAME_FORMAT[]  = "gid+r.%llu.db";
const char XATTR_GID_W_READ_ATTACH_FORMAT[]    = "gidr%llu";
const char XATTR_GID_WO_READ_FILENAME_FORMAT[] = "gid-r.%llu.db";
const char XATTR_GID_WO_READ_ATTACH_FORMAT[]   = "gid%llu";

int xattr_can_rollin(struct stat *parent, struct stat *entry) {
    /* Rule 1: File is 0+R (doesn’t matter what the parent dir perms or ownership is) */
    static const mode_t UGO_MASK = 0444;
    if ((entry->st_mode & UGO_MASK) == UGO_MASK) {
        return 1;
    }

    /* Rule 2: File is UG+R doesn’t matter on other, with file and parent same usr and grp and parent has only UG+R with no other read */
    static const mode_t UG_MASK  = 0440;
    if (((entry->st_mode & UG_MASK) == UG_MASK) &&
        (parent->st_uid == entry->st_uid) &&
        (parent->st_gid == entry->st_gid) &&
        ((parent->st_mode & UGO_MASK) == UG_MASK)) {
        return 2;
    }

    /* Rule 3: File is U+R doesn’t matter on grp and other, with file and parent same usr and parent dir has only U+R, no grp and other read */
    static const mode_t U_MASK  = 0400;
    if (((entry->st_mode & U_MASK) == U_MASK) &&
        (parent->st_uid == entry->st_uid) &&
        (parent->st_gid == entry->st_gid) &&
        ((parent->st_mode & UGO_MASK) == U_MASK)) {
        return 3;
    }

    /* Rule 4: Directory has write for every read: drw*rw_*rw* or drw*rw*___ or drw*______ - if you can write the dir you can chmod the files to see the xattrs */
    if (((parent->st_mode & 0666) == 0666) ||

        /* at least one of uid/gid matches */
        (((parent->st_mode & 0660) == 0660) &&
         ((parent->st_uid == entry->st_uid) ||
          (parent->st_gid == entry->st_gid))) ||

        /* uid matches */
        (((parent->st_mode & 0600) == 0600) &&
         (parent->st_uid == entry->st_uid))) {
        return 4;
    }

    return 0;
}

int xattrs_setup(struct xattrs *xattrs) {
    /* if (!xattrs) { */
    /*     return 1; */
    /* } */

    memset(xattrs, 0, sizeof(*xattrs));
    return 0;
}

/* here to be the opposite function of xattrs_cleanup */
static int xattrs_alloc(struct xattrs *xattrs) {
    if (!xattrs || xattrs->pairs) {
        return 1;
    }

    xattrs->pairs = malloc(sizeof(struct xattr) * xattrs->count);
    return !xattrs->pairs;
}

void xattrs_cleanup(struct xattrs *xattrs) {
    /* if (xattrs) { */
        free(xattrs->pairs);
    /* } */
}

/* ************************************************** */
/* filesystem xattr interactions */

/* caller should zero xattrs since this function isn't always called on a struct xattrs */
int xattrs_get(const char *path, struct xattrs *xattrs) {
    char name_list[MAXXATTR];
    const ssize_t name_list_len = LISTXATTR(path, name_list, sizeof(name_list));
    if (name_list_len < 0) {
        const int err = errno;
        char buf[MAXXATTR];
        getcwd(buf, sizeof(buf));
        fprintf(stderr, "Error: Could not list xattrs for %s: %s (%d) %s\n", path, strerror(err), err, buf);
        return -1;
    }

    /* count xattr names under the namespace user */
    ssize_t offset = 0;
    while (offset < name_list_len) {
        const char *name = name_list + offset;
        const ssize_t name_len = strlen(name);

        if (name_len > 5) {
            if (strncmp(name, "user.", 5) == 0) {
                xattrs->count++;
            }
        }

        offset += name_len + 1; /* + 1 for NULL seperator */
    }

    xattrs_alloc(xattrs);
    /* if (!xattrs->pairs) { */
    /*     return -1; */
    /* } */

    offset = 0;
    for(size_t i = 0; i < xattrs->count;) {
        struct xattr *xattr = &xattrs->pairs[i];
        const char *name = name_list + offset;
        const ssize_t name_len = strlen(name);

        offset += name_len + 1; /* + 1 for NULL seperator */

        if (name_len > 5) {
            if (strncmp(name, "user.", 5) != 0) {
                continue;
            }

            xattr->name_len = SNFORMAT_S(xattr->name, sizeof(xattr->name), 1, name, name_len);
            const ssize_t value_len = GETXATTR(path, xattr->name, xattr->value, sizeof(xattr->value));

            /* man page says positive value or -1 */
            if (value_len > 0) {
                xattr->value_len = value_len;

                xattrs->name_len += xattr->name_len;
                xattrs->len += xattr->name_len + xattr->value_len;

                i++;
            }
            else {
                xattrs->count--;
            }
        }
    }

    return xattrs->count;
}

/*
 * Combine all xattr names into a xattrdelim delimited string.
 */
ssize_t xattr_get_names(const struct xattrs *xattrs, char *buf, size_t buf_len, const char *delim) {
    if (!xattrs || ((xattrs->name_len + xattrs->count) > buf_len)) {
        return -1;
    }

    size_t offset = 0;
    for(size_t i = 0; i < xattrs->count; i++) {
        struct xattr *xattr = &xattrs->pairs[i];
        offset += SNFORMAT_S(buf + offset, buf_len - offset, 2,
                             xattr->name, xattr->name_len,
                             delim, 1);
    }

    return offset;
}
/* ************************************************** */

/* ************************************************** */
/* trace functions */

/*
 * serialize xattrs
 *
 * <name>\x1F<value>\x1F ...
 */
int xattrs_to_file(FILE *file, const struct xattrs *xattrs, const char *delim) {
    if (!xattrs || !xattrs->pairs) {
        return 0;
    }

    int written = 0;
    for(size_t i = 0; i < xattrs->count; i++) {
        struct xattr *xattr = &xattrs->pairs[i];
        written += fwrite(xattr->name, 1, xattr->name_len, file);
        written += fwrite(delim, 1, 1, file);
        written += fwrite(xattr->value, 1, xattr->value_len, file);
        written += fwrite(delim, 1, 1, file);
    }

    return written;
}

/* parse serialized xattrs */
int xattrs_from_line(char *start, const char *end, struct xattrs *xattrs, const char *delim) {
    if (!xattrs) {
        return 0;
    }

    xattrs_setup(xattrs);

    /* count NULL terminators */
    for(char *curr = start; curr != end; curr++) {
        if (*curr == delim[0]) {
            xattrs->count++;
        }
    }
    xattrs->count >>= 1;

    xattrs_alloc(xattrs);

    /* extract pairs */
    char *next = split(start, delim, end);
    for(size_t i = 0; i < xattrs->count; i++) {
        struct xattr *xattr = &xattrs->pairs[i];

        xattr->name_len = SNPRINTF(xattr->name, sizeof(xattr->name), "%s", start);
        start = next; next = split(start, delim, end);

        xattr->value_len = SNPRINTF(xattr->value, sizeof(xattr->value), "%s", start);
        start = next; next = split(start, delim, end);

        xattrs->name_len += xattr->name_len;
        xattrs->len += xattr->name_len + xattr->value_len;
    }

    return xattrs->count;
}
/* ************************************************** */
