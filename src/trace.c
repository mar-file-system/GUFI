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
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "debug.h"
#include "trace.h"
#include "utils.h"
#include "xattrs.h"

int externaltofile(FILE *file, const char delim, const char *path) {
    return fprintf(file, "%s%ce%c\n",
                   path,       delim,
                               delim);
}

int worktofile(FILE *file, const char delim, const size_t prefix_len, struct work *work, struct entry_data *ed) {
    if (!file || !work || !ed) {
        return -1;
    }

    int count = 0;

    count += fwrite(work->name + prefix_len, 1, work->name_len - prefix_len, file);
    count += fwrite(&delim, 1, 1, file);
    count += fwrite(&ed->type, 1, 1, file);
    count += fwrite(&delim, 1, 1, file);
    count += fprintf(file, "%" STAT_ino    "%c", ed->statuso.st_ino,     delim);
    count += fprintf(file, "%" STAT_mode   "%c", ed->statuso.st_mode,    delim);
    count += fprintf(file, "%" STAT_nlink  "%c", ed->statuso.st_nlink,   delim);
    count += fprintf(file, "%" STAT_uid    "%c", ed->statuso.st_uid,     delim);
    count += fprintf(file, "%" STAT_gid    "%c", ed->statuso.st_gid,     delim);
    count += fprintf(file, "%" STAT_size   "%c", ed->statuso.st_size,    delim);
    count += fprintf(file, "%" STAT_bsize  "%c", ed->statuso.st_blksize, delim);
    count += fprintf(file, "%" STAT_blocks "%c", ed->statuso.st_blocks,  delim);
    count += fprintf(file, "%ld%c",              ed->statuso.st_atime,   delim);
    count += fprintf(file, "%ld%c",              ed->statuso.st_mtime,   delim);
    count += fprintf(file, "%ld%c",              ed->statuso.st_ctime,   delim);
    count += fprintf(file, "%s%c",               ed->linkname,           delim);
    count += xattrs_to_file(file, &ed->xattrs, XATTRDELIM);
    count += fprintf(file, "%c",                                         delim);
    count += fprintf(file, "%d%c",               ed->crtime,             delim);
    count += fprintf(file, "%d%c",               ed->ossint1,            delim);
    count += fprintf(file, "%d%c",               ed->ossint2,            delim);
    count += fprintf(file, "%d%c",               ed->ossint3,            delim);
    count += fprintf(file, "%d%c",               ed->ossint4,            delim);
    count += fprintf(file, "%s%c",               ed->osstext1,           delim);
    count += fprintf(file, "%s%c",               ed->osstext2,           delim);
    count += fprintf(file, "%lld%c",             work->pinode,           delim);
    count += fprintf(file, "\n");

    return count;
}

int linetowork(char *line, const size_t len, const char delim,
               struct work *work, struct entry_data *ed) {
    if (!line || !work || !ed) {
        return -1;
    }

    const char *end = line + len;

    char *p;
    char *q;

    p=line; q = split(p, &delim, 1, end); work->name_len = SNPRINTF(work->name, MAXPATH, "%s", p);
    p = q;  q = split(p, &delim, 1, end); ed->type = *p;

    if (ed->type == 'e') {
        return 0;
    }

    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_ino, &ed->statuso.st_ino);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_mode, &ed->statuso.st_mode);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_nlink, &ed->statuso.st_nlink);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_uid, &ed->statuso.st_uid);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_gid, &ed->statuso.st_gid);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_size, &ed->statuso.st_size);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_bsize, &ed->statuso.st_blksize);
    p = q; q = split(p, &delim, 1, end); sscanf(p, "%" STAT_blocks, &ed->statuso.st_blocks);
    p = q; q = split(p, &delim, 1, end); ed->statuso.st_atime = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->statuso.st_mtime = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->statuso.st_ctime = atol(p);
    p = q; q = split(p, &delim, 1, end); SNPRINTF(ed->linkname,MAXPATH, "%s", p);
    p = q; q = split(p, &delim, 1, end); xattrs_from_line(p, q - 1, &ed->xattrs, XATTRDELIM);
    p = q; q = split(p, &delim, 1, end); ed->crtime = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint1 = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint2 = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint3 = atol(p);
    p = q; q = split(p, &delim, 1, end); ed->ossint4 = atol(p);
    p = q; q = split(p, &delim, 1, end); SNPRINTF(ed->osstext1, MAXXATTR, "%s", p);
    p = q; q = split(p, &delim, 1, end); SNPRINTF(ed->osstext2, MAXXATTR, "%s", p);
    p = q;     split(p, &delim, 1, end); work->pinode = atol(p);

    work->basename_len = work->name_len - trailing_match_index(work->name, work->name_len, "/", 1);

    return 0;
}

int *open_traces(char **trace_names, size_t trace_count) {
    int *traces = (int *) calloc(trace_count, sizeof(int));
    if (traces) {
        for(size_t i = 0; i < trace_count; i++) {
            traces[i] = open(trace_names[i], O_RDONLY);
            if (traces[i] < 0) {
                const int err = errno;
                fprintf(stderr, "Could not open \"%s\": %s (%d)\n", trace_names[i], strerror(err), err);
                close_traces(traces, i);
                return NULL;
            }
        }
    }

    return traces;
}

void close_traces(int *traces, size_t trace_count) {
    for(size_t i = 0; i < trace_count; i++) {
        close(traces[i]);
    }
    free(traces);
}

struct row *row_init(const int trace, const size_t first_delim, char *line,
                     const size_t len, const long offset) {
    struct row *row = malloc(sizeof(struct row));
    row->trace = trace;
    row->first_delim = first_delim;
    row->line = line; /* takes ownership of line */
    row->len = len;
    row->offset = offset;
    row->entries = 0;
    return row;
}

void row_destroy(struct row **ref) {
    /* Not checking arguments */

    struct row *row = *ref;
    free(row->line);
    free(row);

    *ref = NULL;
}

/* Read ahead to figure out where files under directories start */
int scout_trace(QPTPool_t *ctx, const size_t id, void *data, void *args) {
    struct start_end scouting;
    clock_gettime(CLOCK_MONOTONIC, &scouting.start);

    /* skip argument checking */
    struct ScoutTraceArgs *sta = (struct ScoutTraceArgs *) data;

    (void) id; (void) args;

    int rc = 0;

    size_t file_count = 0;
    size_t dir_count = 0;
    size_t empty = 0;
    size_t external = 0;

    char *line = NULL;
    size_t size = 0;
    ssize_t len = 0;
    off_t offset = 0;

    /* keep current directory while finding next directory */
    /* in order to find out whether or not the current */
    /* directory has files in it */

    /* empty trace */
    if ((len = getline_fd(&line, &size, sta->trace, &offset, GETLINE_DEFAULT_SIZE)) < 1) {
        free(line);
        fprintf(stderr, "Could not get the first line of trace \"%s\"\n", sta->tracename);
        rc = 1;
        goto done;
    }

    /* find a delimiter */
    char *first_delim = memchr(line, sta->delim, len);
    if (!first_delim) {
        free(line);
        fprintf(stderr, "Could not find the specified delimiter in \"%s\"\n", sta->tracename);
        rc = 1;
        goto done;
    }

    /* make sure the first line is a directory */
    if (first_delim[1] != 'd') {
        free(line);
        fprintf(stderr, "First line of \"%s\" is not a directory\n", sta->tracename);
        rc = 1;
        goto done;
    }

    struct row *work = row_init(sta->trace, first_delim - line, line, len, offset);

    /* don't free line - the pointer is now owned by work */

    /* have getline allocate a new buffer */
    line = NULL;
    size = 0;
    len = 0;
    while ((len = getline_fd(&line, &size, sta->trace, &offset, GETLINE_DEFAULT_SIZE)) > 0) {
        first_delim = memchr(line, sta->delim, len);

        /*
         * if got bad line, have to stop here or else processdir will
         * not know where this directory ends and will try to parse
         * bad line
         */
        if (!first_delim) {
            free(line);
            row_destroy(&work);
            fprintf(stderr, "Scout encountered bad line ending at \"%s\" offset %jd\n",
                    sta->tracename, (intmax_t) offset);
            rc = 1;
            goto done;
        }

        /* push directories onto queues */
        if (first_delim[1] == 'd') {
            dir_count++;

            /* external dbs do not contribue to entry count, but are needed to loop correctly when processing directory */
            empty += !work->entries;
            work->entries += external;
            external = 0;

            /* put the previous work on the queue */
            QPTPool_enqueue(ctx, id, sta->processdir, work);

            /* put the current line into a new work item */
            work = row_init(sta->trace, first_delim - line, line, len, offset);
        }
        /* ignore non-directories */
        else {
            if (first_delim[1] == 'e') {
                external++;
            }
            else {
                work->entries++;
                file_count++;
            }

            /* this line is not needed */
            free(line);
        }

        /* have getline allocate a new buffer */
        line = NULL;
        size = 0;
        len = 0;
    }

    free(line);

    /* handle the last work item */
    dir_count++;
    /* external dbs do not contribue to entry count, but are needed to loop correctly when processing directory */
    empty += !work->entries;
    work->entries += external;

    QPTPool_enqueue(ctx, id, sta->processdir, work);

  done:
    clock_gettime(CLOCK_MONOTONIC, &scouting.end);

    pthread_mutex_lock(sta->mutex);
    *sta->time  += nsec(&scouting);
    *sta->files += file_count;
    *sta->dirs  += dir_count;
    *sta->empty += empty;

    (*sta->remaining)--;

    /* print here to print as early as possible instead of after thread pool completes */
    if ((*sta->remaining) == 0) {
        fprintf(stdout, "Scouts took total of %.2Lf seconds\n", sec(*sta->time));
        fprintf(stdout, "Dirs:                %zu (%zu empty)\n", *sta->dirs, *sta->empty);
        fprintf(stdout, "Files:               %zu\n", *sta->files);
        fprintf(stdout, "Total:               %zu\n", *sta->files + *sta->dirs);
        fprintf(stdout, "\n");
    }
    pthread_mutex_unlock(sta->mutex);

    if (sta->free) {
        sta->free(sta);
    }

    return rc;
}
