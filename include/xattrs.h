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



#ifndef GUFI_XATTR_H
#define GUFI_XATTR_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <sys/stat.h>

/* Include platform-specific xattr */
#if CONFIG_SYS_XATTR_H
#include <sys/types.h>
#include <sys/xattr.h>
#elif CONFIG_ATTR_XATTR_H
#include <attr/xattr.h>
#else
#include <sys/xattr.h>
#endif

/* Alias xattr functions to standard subset of GNU and BSD */
#ifdef CONFIG_GNU_XATTRS
#  define LISTXATTR(PATH, BUF, SIZE)        llistxattr((PATH), (BUF), (SIZE))
#  define GETXATTR(PATH, KEY, BUF, SIZE)    lgetxattr((PATH), (KEY), (BUF), (SIZE))
#  define SETXATTR(PATH, KEY, VALUE, SIZE)  lsetxattr((PATH), (KEY), (VALUE), (SIZE), 0)

#elif CONFIG_DARWIN_XATTRS
#  define LISTXATTR(PATH, BUF, SIZE)        listxattr((PATH), (BUF), (SIZE), XATTR_NOFOLLOW)
#  define GETXATTR(PATH, KEY, BUF, SIZE)    getxattr((PATH), (KEY), (BUF), (SIZE), 0, XATTR_NOFOLLOW)
#  define SETXATTR(PATH, KEY, VALUE, SIZE)  setxattr((PATH), (KEY), (VALUE), (SIZE), 0, 0)
#else

#endif

#define MAXXATTR          1024
extern const char XATTRDELIM[];

/* each db.db, per-user db, and per-group db will have a table with this name */
#define XATTRS_TABLE_NAME      "xattrs_avail"

/* the view used by the caller */
#define XATTRS_VIEW_NAME       "xattrs"

/* queries used to set up the xattr tables */
extern const char XATTRS_SQL_CREATE[];
extern const char XATTRS_SQL_INSERT[];

/* table containing list of per-user and per-group xattr dbs */
#define XATTR_FILES_NAME       "xattr_files"

/* format to pass into snprintf */
extern const char XATTR_UID_FILENAME_FORMAT[];
extern const char XATTR_UID_ATTACH_FORMAT[];
extern const char XATTR_GID_W_READ_FILENAME_FORMAT[];
extern const char XATTR_GID_W_READ_ATTACH_FORMAT[];
extern const char XATTR_GID_WO_READ_FILENAME_FORMAT[];
extern const char XATTR_GID_WO_READ_ATTACH_FORMAT[];

/* SQL statements used to set up the xattr files table */
extern const char XATTR_FILES_SQL_CREATE[];
extern const char XATTR_FILES_SQL_INSERT[];

/* single xattr name-value pair */
struct xattr {
    char   name[MAXXATTR];
    size_t name_len;
    char   value[MAXXATTR];
    size_t value_len;
};

/* check if entry xattr can be put into db.db */
int xattr_can_rollin(struct stat *parent, struct stat *entry);

/* list of xattr pairs */
struct xattrs {
    struct xattr *pairs;
    size_t        name_len; /* name lengths only - no separators */
    size_t        len;      /* name + value lengths only - no separators */
    size_t        count;
};

/* sets up and clean up internal structure only, not the pointer itself */
int xattrs_setup(struct xattrs *xattrs);
void xattrs_cleanup(struct xattrs *xattrs);

/* filesystem xattr interactions */
int xattrs_get(const char *path, struct xattrs *xattrs);
ssize_t xattr_get_names(const struct xattrs *xattrs, char *buf, size_t buf_len, const char *delim);

/* GUFI trace functions */
int xattrs_to_file(FILE *file, const struct xattrs *xattrs, const char *delim);
int xattrs_from_line(char *start, const char *end, struct xattrs *xattrs, const char *delim);

#ifdef __cplusplus
}
#endif

#endif
