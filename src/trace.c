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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int worktofile(FILE * file, char * delim, struct work * work) {
    if (!file || !delim || !work) {
        return -1;
    }

    int count = 0;

    count += fprintf(file, "%s%c",               work->name,               delim[0]);
    count += fprintf(file, "%c%c",               work->type[0],            delim[0]);
    count += fprintf(file, "%" STAT_ino "%c",    work->statuso.st_ino,     delim[0]);
    count += fprintf(file, "%d%c",               work->statuso.st_mode,    delim[0]);
    count += fprintf(file, "%" STAT_nlink"%c",   work->statuso.st_nlink,   delim[0]);
    count += fprintf(file, "%d%c",               work->statuso.st_uid,     delim[0]);
    count += fprintf(file, "%d%c",               work->statuso.st_gid,     delim[0]);
    count += fprintf(file, "%" STAT_size "%c",   work->statuso.st_size,    delim[0]);
    count += fprintf(file, "%" STAT_bsize "%c",  work->statuso.st_blksize, delim[0]);
    count += fprintf(file, "%" STAT_blocks "%c", work->statuso.st_blocks,  delim[0]);
    count += fprintf(file, "%ld%c",              work->statuso.st_atime,   delim[0]);
    count += fprintf(file, "%ld%c",              work->statuso.st_mtime,   delim[0]);
    count += fprintf(file, "%ld%c",              work->statuso.st_ctime,   delim[0]);
    count += fprintf(file, "%s%c",               work->linkname,           delim[0]);
    count += fprintf(file, "%s%c",               work->xattr,              delim[0]);
    count += fprintf(file, "%d%c",               work->crtime,             delim[0]);
    count += fprintf(file, "%d%c",               work->ossint1,            delim[0]);
    count += fprintf(file, "%d%c",               work->ossint2,            delim[0]);
    count += fprintf(file, "%d%c",               work->ossint3,            delim[0]);
    count += fprintf(file, "%d%c",               work->ossint4,            delim[0]);
    count += fprintf(file, "%s%c",               work->osstext1,           delim[0]);
    count += fprintf(file, "%s%c",               work->osstext2,           delim[0]);
    count += fprintf(file, "%lld%c",             work->pinode,             delim[0]);
    count += fprintf(file, "\n");

    return count;
}

int filetowork(FILE * file, char * delim, struct work * work) {
    if (!file || !delim || !work) {
        return -1;
    }

    char * line = NULL;
    size_t n = 0;
    if (getline(&line, &n, file) == -1) {
        return -1;
    }

    int rc = linetowork(line, delim, work);
    free(line);

    return rc;
}

int linetowork(char * line, char * delim, struct work * work) {
    if (!line || !delim || !work) {
        return -1;
    }

    char *p;
    char *q;

    line[strlen(line)-1]= '\0';
    p=line;    q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(work->name,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(work->type,2,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_ino=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_mode=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_nlink=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_uid=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_gid=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_size=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_blksize=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_blocks=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_atime=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_mtime=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->statuso.st_ctime=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(work->linkname,MAXPATH,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(work->xattr,MAXXATTR,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->crtime=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->ossint1=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->ossint2=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->ossint3=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->ossint4=atol(p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(work->osstext1,MAXXATTR,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); SNPRINTF(work->osstext2,MAXXATTR,"%s",p);
    p=q+1;     q=strstr(p,delim); memset(q, 0, 1); work->pinode=atol(p);

    return 0;
}
