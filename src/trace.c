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



#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "trace.h"
#include "utils.h"
#include "xattrs.h"

int worktofile(FILE *file, const char delim, const size_t prefix_len, struct work *work, struct entry_data *ed) {
    if (!file || !work || !ed) {
        return -1;
    }

    int count = 0;

    count += fwrite(work->name + prefix_len, 1, work->name_len - prefix_len, file);
    count += fwrite(&delim, 1, 1, file);
    count += fprintf(file, "%c%c",                    ed->type,               delim);
    count += fprintf(file, "%" STAT_ino "%c",         ed->statuso.st_ino,     delim);
    count += fprintf(file, "%d%c",                    ed->statuso.st_mode,    delim);
    count += fprintf(file, "%" STAT_nlink "%c",       ed->statuso.st_nlink,   delim);
    count += fprintf(file, "%" PRId64 "%c", (int64_t) ed->statuso.st_uid,     delim);
    count += fprintf(file, "%" PRId64 "%c", (int64_t) ed->statuso.st_gid,     delim);
    count += fprintf(file, "%" STAT_size "%c",        ed->statuso.st_size,    delim);
    count += fprintf(file, "%" STAT_bsize "%c",       ed->statuso.st_blksize, delim);
    count += fprintf(file, "%" STAT_blocks "%c",      ed->statuso.st_blocks,  delim);
    count += fprintf(file, "%ld%c",                   ed->statuso.st_atime,   delim);
    count += fprintf(file, "%ld%c",                   ed->statuso.st_mtime,   delim);
    count += fprintf(file, "%ld%c",                   ed->statuso.st_ctime,   delim);
    count += fprintf(file, "%s%c",                    ed->linkname,           delim);
    count += xattrs_to_file(file, &ed->xattrs, XATTRDELIM);
    count += fprintf(file, "%c",                                              delim);
    count += fprintf(file, "%d%c",                    ed->crtime,             delim);
    count += fprintf(file, "%d%c",                    ed->ossint1,            delim);
    count += fprintf(file, "%d%c",                    ed->ossint2,            delim);
    count += fprintf(file, "%d%c",                    ed->ossint3,            delim);
    count += fprintf(file, "%d%c",                    ed->ossint4,            delim);
    count += fprintf(file, "%s%c",                    ed->osstext1,           delim);
    count += fprintf(file, "%s%c",                    ed->osstext2,           delim);
    count += fprintf(file, "%lld%c",                  work->pinode,           delim);
    count += fprintf(file, "\n");

    return count;
}

int linetowork(char *line, const size_t len, const char delim, struct work *work, struct entry_data *ed) {
    if (!line || !work || !ed) {
        return -1;
    }

    const char *end = line + len;

    char *p;
    char *q;
    int64_t uid = -1;
    int64_t gid = -1;

    p=line; q = split(p, &delim, 1, end); work->name_len = SNPRINTF(work->name, MAXPATH, "%s", p);
    p = q;  q = split(p, &delim, 1, end); ed->type = *p;
    p = q;  q = split(p, &delim, 1, end); sscanf(p, "%" STAT_ino, &ed->statuso.st_ino);
    p = q;  q = split(p, &delim, 1, end); ed->statuso.st_mode = atol(p);
    p = q;  q = split(p, &delim, 1, end); sscanf(p, "%" STAT_nlink, &ed->statuso.st_nlink);
    p = q;  q = split(p, &delim, 1, end); sscanf(p, "%" PRId64, &uid); ed->statuso.st_uid = uid;
    p = q;  q = split(p, &delim, 1, end); sscanf(p, "%" PRId64, &gid); ed->statuso.st_gid = gid;
    p = q;  q = split(p, &delim, 1, end); sscanf(p, "%" STAT_size, &ed->statuso.st_size);
    p = q;  q = split(p, &delim, 1, end); sscanf(p, "%" STAT_bsize, &ed->statuso.st_blksize);
    p = q;  q = split(p, &delim, 1, end); sscanf(p, "%" STAT_blocks, &ed->statuso.st_blocks);
    p = q;  q = split(p, &delim, 1, end); ed->statuso.st_atime = atol(p);
    p = q;  q = split(p, &delim, 1, end); ed->statuso.st_mtime = atol(p);
    p = q;  q = split(p, &delim, 1, end); ed->statuso.st_ctime = atol(p);
    p = q;  q = split(p, &delim, 1, end); SNPRINTF(ed->linkname,MAXPATH, "%s", p);
    p = q;  q = split(p, &delim, 1, end); xattrs_from_line(p, q - 1, &ed->xattrs, XATTRDELIM);
    p = q;  q = split(p, &delim, 1, end); ed->crtime = atol(p);
    p = q;  q = split(p, &delim, 1, end); ed->ossint1 = atol(p);
    p = q;  q = split(p, &delim, 1, end); ed->ossint2 = atol(p);
    p = q;  q = split(p, &delim, 1, end); ed->ossint3 = atol(p);
    p = q;  q = split(p, &delim, 1, end); ed->ossint4 = atol(p);
    p = q;  q = split(p, &delim, 1, end); SNPRINTF(ed->osstext1, MAXXATTR, "%s", p);
    p = q;  q = split(p, &delim, 1, end); SNPRINTF(ed->osstext2, MAXXATTR, "%s", p);
    p = q;      split(p, &delim, 1, end); work->pinode = atol(p);

    work->basename_len = work->name_len - trailing_match_index(work->name, work->name_len, "/", 1);

    return 0;
}
