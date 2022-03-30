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



#include "trace.h"
#include "utils.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int worktofile(FILE * file, char * delim, struct work * work) {
    if (!file || !delim || !work) {
        return -1;
    }

    int count = 0;

    count += fprintf(file, "%s%c",                    work->name,               delim[0]);
    count += fprintf(file, "%c%c",                    work->type[0],            delim[0]);
    count += fprintf(file, "%" STAT_ino "%c",         work->statuso.st_ino,     delim[0]);
    count += fprintf(file, "%d%c",                    work->statuso.st_mode,    delim[0]);
    count += fprintf(file, "%" STAT_nlink "%c",       work->statuso.st_nlink,   delim[0]);
    count += fprintf(file, "%" PRId64 "%c", (int64_t) work->statuso.st_uid,     delim[0]);
    count += fprintf(file, "%" PRId64 "%c", (int64_t) work->statuso.st_gid,     delim[0]);
    count += fprintf(file, "%" STAT_size "%c",        work->statuso.st_size,    delim[0]);
    count += fprintf(file, "%" STAT_bsize "%c",       work->statuso.st_blksize, delim[0]);
    count += fprintf(file, "%" STAT_blocks "%c",      work->statuso.st_blocks,  delim[0]);
    count += fprintf(file, "%ld%c",                   work->statuso.st_atime,   delim[0]);
    count += fprintf(file, "%ld%c",                   work->statuso.st_mtime,   delim[0]);
    count += fprintf(file, "%ld%c",                   work->statuso.st_ctime,   delim[0]);
    count += fprintf(file, "%s%c",                    work->linkname,           delim[0]);
    fwrite(work->xattrs, sizeof(char), work->xattrs_len, file);
    count += work->xattrs_len;
    count += fprintf(file, "%c",                                                delim[0]);
    count += fprintf(file, "%d%c",                    work->crtime,             delim[0]);
    count += fprintf(file, "%d%c",                    work->ossint1,            delim[0]);
    count += fprintf(file, "%d%c",                    work->ossint2,            delim[0]);
    count += fprintf(file, "%d%c",                    work->ossint3,            delim[0]);
    count += fprintf(file, "%d%c",                    work->ossint4,            delim[0]);
    count += fprintf(file, "%s%c",                    work->osstext1,           delim[0]);
    count += fprintf(file, "%s%c",                    work->osstext2,           delim[0]);
    count += fprintf(file, "%lld%c",                  work->pinode,             delim[0]);
    count += fprintf(file, "\n");

    return count;
}

int filetowork(FILE * file, char * delim, struct work * work) {
    if (!file || !delim || !work) {
        return -1;
    }

    char * line = NULL;
    size_t len = 0;
    if (getline(&line, &len, file) == -1) {
        return -1;
    }

    const int rc = linetowork(line, len, delim, work);
    free(line);

    return rc;
}

/* strstr replacement */
/* does not terminate on NULL character */
/* automatically adds NULL character to matching delimiter */
static char * split(char * src, char * delim, const char * end) {
    while (src < end) {
        for(char * d = delim; *d; d++) {
            if (*src == *d) {
                *src = '\x00';
                return src + 1;
            }
        }

        src++;
    }

    return NULL;
}

int linetowork(char * line, const size_t len, char * delim, struct work * work) {
    if (!line || !delim || !work) {
        return -1;
    }

    const char * end = line + len;

    char *p;
    char *q;
    int64_t uid = -1;
    int64_t gid = -1;

    p=line; q = split(p, delim, end); SNPRINTF(work->name,MAXPATH,"%s",p);
    p = q;  q = split(p, delim, end); SNPRINTF(work->type,2,"%s",p);
    p = q;  q = split(p, delim, end); sscanf(p, "%" STAT_ino, &work->statuso.st_ino);
    p = q;  q = split(p, delim, end); work->statuso.st_mode=atol(p);
    p = q;  q = split(p, delim, end); sscanf(p, "%" STAT_nlink, &work->statuso.st_nlink);
    p = q;  q = split(p, delim, end); sscanf(p, "%" PRId64, &uid); work->statuso.st_uid = uid;
    p = q;  q = split(p, delim, end); sscanf(p, "%" PRId64, &gid); work->statuso.st_gid = gid;
    p = q;  q = split(p, delim, end); sscanf(p, "%" STAT_size, &work->statuso.st_size);
    p = q;  q = split(p, delim, end); sscanf(p, "%" STAT_bsize, &work->statuso.st_blksize);
    p = q;  q = split(p, delim, end); sscanf(p, "%" STAT_blocks, &work->statuso.st_blocks);
    p = q;  q = split(p, delim, end); work->statuso.st_atime=atol(p);
    p = q;  q = split(p, delim, end); work->statuso.st_mtime=atol(p);
    p = q;  q = split(p, delim, end); work->statuso.st_ctime=atol(p);
    p = q;  q = split(p, delim, end); SNPRINTF(work->linkname,MAXPATH,"%s",p);
    p = q;  q = split(p, delim, end); work->xattrs_len = SNPRINTF(work->xattrs,MAXPATH,"%s",p);
    p = q;  q = split(p, delim, end); work->crtime=atol(p);
    p = q;  q = split(p, delim, end); work->ossint1=atol(p);
    p = q;  q = split(p, delim, end); work->ossint2=atol(p);
    p = q;  q = split(p, delim, end); work->ossint3=atol(p);
    p = q;  q = split(p, delim, end); work->ossint4=atol(p);
    p = q;  q = split(p, delim, end); SNPRINTF(work->osstext1,MAXXATTR,"%s",p);
    p = q;  q = split(p, delim, end); SNPRINTF(work->osstext2,MAXXATTR,"%s",p);
    p = q;  q = split(p, delim, end); work->pinode=atol(p);

    return 0;
}
