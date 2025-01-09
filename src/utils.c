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
#include <limits.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utime.h>

#include "external.h"
#include "utils.h"

uint64_t get_queue_limit(const uint64_t target_memory_footprint, const uint64_t nthreads) {
    /* not set */
    if (target_memory_footprint == 0) {
        return 0;
    }

    /* set, so queue_limit should be at minimum 1 */
    return max(target_memory_footprint / sizeof(struct work) / nthreads, 1);
}

int zeroit(struct sum *summary)
{
  summary->totfiles=0;
  summary->totlinks=0;
  summary->minuid=LLONG_MAX;
  summary->maxuid=LLONG_MIN;
  summary->mingid=LLONG_MAX;
  summary->maxgid=LLONG_MIN;
  summary->minsize=LLONG_MAX;
  summary->maxsize=LLONG_MIN;
  summary->totzero=0;
  summary->totltk=0;
  summary->totmtk=0;
  summary->totltm=0;
  summary->totmtm=0;
  summary->totmtg=0;
  summary->totmtt=0;
  summary->totsize=0;
  summary->minctime=LLONG_MAX;
  summary->maxctime=LLONG_MIN;
  summary->minmtime=LLONG_MAX;
  summary->maxmtime=LLONG_MIN;
  summary->minatime=LLONG_MAX;
  summary->maxatime=LLONG_MIN;
  summary->minblocks=LLONG_MAX;
  summary->maxblocks=LLONG_MIN;
  summary->totxattr=0;
  summary->totsubdirs=0;
  summary->maxsubdirfiles=LLONG_MIN;
  summary->maxsubdirlinks=LLONG_MIN;
  summary->maxsubdirsize=LLONG_MIN;
  summary->mincrtime=LLONG_MAX;
  summary->maxcrtime=LLONG_MIN;
  summary->minossint1=LLONG_MAX;
  summary->maxossint1=LLONG_MIN;
  summary->totossint1=0;
  summary->minossint2=LLONG_MAX;
  summary->maxossint2=LLONG_MIN;
  summary->totossint2=0;
  summary->minossint3=LLONG_MAX;
  summary->maxossint3=LLONG_MIN;
  summary->totossint3=0;
  summary->minossint4=LLONG_MAX;
  summary->maxossint4=LLONG_MIN;
  summary->totossint4=0;
  summary->totextdbs=0;
  return 0;
}

int sumit(struct sum *summary, struct entry_data *ed) {
  if (ed->type == 'f') {
     summary->totfiles++;
     if (ed->statuso.st_size < summary->minsize) summary->minsize=ed->statuso.st_size;
     if (ed->statuso.st_size > summary->maxsize) summary->maxsize=ed->statuso.st_size;
     if (ed->statuso.st_size == 0) summary->totzero++;
     if (ed->statuso.st_size <= 1024) summary->totltk++;
     if (ed->statuso.st_size > 1024) summary->totmtk++;
     if (ed->statuso.st_size <= 1048576) summary->totltm++;
     if (ed->statuso.st_size > 1048576) summary->totmtm++;
     if (ed->statuso.st_size > 1073741824) summary->totmtg++;
     if (ed->statuso.st_size > 1099511627776) summary->totmtt++;
     summary->totsize=summary->totsize+ed->statuso.st_size;

     if (ed->statuso.st_blocks < summary->minblocks) summary->minblocks=ed->statuso.st_blocks;
     if (ed->statuso.st_blocks > summary->maxblocks) summary->maxblocks=ed->statuso.st_blocks;
  }
  if (ed->type == 'l') {
     summary->totlinks++;
  }

  if (ed->statuso.st_uid < summary->minuid) summary->minuid=ed->statuso.st_uid;
  if (ed->statuso.st_uid > summary->maxuid) summary->maxuid=ed->statuso.st_uid;
  if (ed->statuso.st_gid < summary->mingid) summary->mingid=ed->statuso.st_gid;
  if (ed->statuso.st_gid > summary->maxgid) summary->maxgid=ed->statuso.st_gid;
  if (ed->statuso.st_ctime < summary->minctime) summary->minctime=ed->statuso.st_ctime;
  if (ed->statuso.st_ctime > summary->maxctime) summary->maxctime=ed->statuso.st_ctime;
  if (ed->statuso.st_mtime < summary->minmtime) summary->minmtime=ed->statuso.st_mtime;
  if (ed->statuso.st_mtime > summary->maxmtime) summary->maxmtime=ed->statuso.st_mtime;
  if (ed->statuso.st_atime < summary->minatime) summary->minatime=ed->statuso.st_atime;
  if (ed->statuso.st_atime > summary->maxatime) summary->maxatime=ed->statuso.st_atime;
  if (ed->crtime < summary->mincrtime) summary->mincrtime=ed->crtime;
  if (ed->crtime > summary->maxcrtime) summary->maxcrtime=ed->crtime;

  if (ed->ossint1 < summary->minossint1) summary->minossint1=ed->ossint1;
  if (ed->ossint1 > summary->maxossint1) summary->maxossint1=ed->ossint1;
  summary->totossint1=summary->totossint1+ed->ossint1;

  if (ed->ossint2 < summary->minossint2) summary->minossint2=ed->ossint2;
  if (ed->ossint2 > summary->maxossint2) summary->maxossint2=ed->ossint2;
  summary->totossint2=summary->totossint2+ed->ossint2;

  if (ed->ossint3 < summary->minossint3) summary->minossint3=ed->ossint3;
  if (ed->ossint3 > summary->maxossint3) summary->maxossint3=ed->ossint3;
  summary->totossint3=summary->totossint3+ed->ossint3;

  if (ed->ossint4 < summary->minossint4) summary->minossint4=ed->ossint4;
  if (ed->ossint4 > summary->maxossint4) summary->maxossint4=ed->ossint4;
  summary->totossint4=summary->totossint4+ed->ossint4;

  summary->totxattr += ed->xattrs.count;

  return 0;
}

int tsumit(struct sum *sumin, struct sum *smout) {
  /* count self as subdirectory because can't tell from here where treesummary descent started */
  smout->totsubdirs++;

  /* if sumin totsubdirs is > 0 we are summing a treesummary not a dirsummary */
  if (sumin->totsubdirs > 0) {
    smout->totsubdirs += sumin->totsubdirs;
    if (sumin->maxsubdirfiles > smout->maxsubdirfiles) smout->maxsubdirfiles = sumin->maxsubdirfiles;
    if (sumin->maxsubdirlinks > smout->maxsubdirlinks) smout->maxsubdirlinks = sumin->maxsubdirlinks;
    if (sumin->maxsubdirsize  > smout->maxsubdirsize)  smout->maxsubdirsize  = sumin->maxsubdirsize;
  } else {
    if (sumin->totfiles > smout->maxsubdirfiles) smout->maxsubdirfiles = sumin->totfiles;
    if (sumin->totlinks > smout->maxsubdirlinks) smout->maxsubdirlinks = sumin->totlinks;
    if (sumin->totsize  > smout->maxsubdirsize)  smout->maxsubdirsize  = sumin->totsize;
  }

  smout->totfiles += sumin->totfiles;
  smout->totlinks += sumin->totlinks;

  if (sumin->totfiles > 0) {
  /* only set these mins and maxes if there are files in the directory
     otherwise mins are all zero */
  //if (sumin->totfiles > 0) {
    if (sumin->minuid < smout->minuid) smout->minuid=sumin->minuid;
    if (sumin->maxuid > smout->maxuid) smout->maxuid=sumin->maxuid;
    if (sumin->mingid < smout->mingid) smout->mingid=sumin->mingid;
    if (sumin->maxgid > smout->maxgid) smout->maxgid=sumin->maxgid;
    if (sumin->minsize < smout->minsize) smout->minsize=sumin->minsize;
    if (sumin->maxsize > smout->maxsize) smout->maxsize=sumin->maxsize;
    if (sumin->minblocks < smout->minblocks) smout->minblocks=sumin->minblocks;
    if (sumin->maxblocks > smout->maxblocks) smout->maxblocks=sumin->maxblocks;
    if (sumin->minctime < smout->minctime) smout->minctime=sumin->minctime;
    if (sumin->maxctime > smout->maxctime) smout->maxctime=sumin->maxctime;
    if (sumin->minmtime < smout->minmtime) smout->minmtime=sumin->minmtime;
    if (sumin->maxmtime > smout->maxmtime) smout->maxmtime=sumin->maxmtime;
    if (sumin->minatime < smout->minatime) smout->minatime=sumin->minatime;
    if (sumin->maxatime > smout->maxatime) smout->maxatime=sumin->maxatime;
    if (sumin->mincrtime < smout->mincrtime) smout->mincrtime=sumin->mincrtime;
    if (sumin->maxcrtime > smout->maxcrtime) smout->maxcrtime=sumin->maxcrtime;
    if (sumin->minossint1 < smout->minossint1) smout->minossint1=sumin->minossint1;
    if (sumin->maxossint1 > smout->maxossint1) smout->maxossint1=sumin->maxossint1;
    if (sumin->minossint2 < smout->minossint2) smout->minossint2=sumin->minossint2;
    if (sumin->maxossint2 > smout->maxossint2) smout->maxossint2=sumin->maxossint2;
    if (sumin->minossint3 < smout->minossint3) smout->minossint3=sumin->minossint3;
    if (sumin->maxossint3 > smout->maxossint3) smout->maxossint3=sumin->maxossint3;
    if (sumin->minossint4 < smout->minossint4) smout->minossint4=sumin->minossint4;
    if (sumin->maxossint4 > smout->maxossint4) smout->maxossint4=sumin->maxossint4;
  }

  smout->totzero    += sumin->totzero;
  smout->totltk     += sumin->totltk;
  smout->totmtk     += sumin->totmtk;
  smout->totltm     += sumin->totltm;
  smout->totmtm     += sumin->totmtm;
  smout->totmtg     += sumin->totmtg;
  smout->totmtt     += sumin->totmtt;
  smout->totsize    += sumin->totsize;
  smout->totxattr   += sumin->totxattr;
  smout->totossint1 += sumin->totossint1;
  smout->totossint2 += sumin->totossint2;
  smout->totossint3 += sumin->totossint3;
  smout->totossint4 += sumin->totossint4;

  smout->totextdbs  += sumin->totextdbs;

  return 0;
}

// given a possibly-multi-level path of directories (final component is
// also a dir), create the parent dirs all the way down.
//
int mkpath(char *path, mode_t mode, uid_t uid, gid_t gid) {
    for (char *p = strchr(path + 1, '/'); p; p = strchr(p + 1, '/')) {
        *p = '\0';
        if (mkdir(path, mode) == -1) {
            const int err = errno;
            if (err != EEXIST) {
                *p = '/';
                return -1;
            }
        }
        else {
            chmod(path, mode);
            chown(path, uid, gid);
        }
        *p = '/';
    }
    return mkdir(path,mode);
}

int dupdir(const char *path, struct stat *stat) {
    /* if dupdir is called, doing a memory copy will not be relatively heavyweight */
    const size_t path_len = strlen(path);
    char copy[MAXPATH];
    SNFORMAT_S(copy, sizeof(copy), 1,
               path, path_len);
    copy[path_len] = '\0';

    // the writer must be able to create the index files into this directory so or in S_IWRITE
    if (mkdir(copy, stat->st_mode) != 0) {
        const int err = errno;
        if (err == ENOENT) {
            mkpath(copy, stat->st_mode, stat->st_uid, stat->st_gid);
        }
        else if (err == EEXIST) {
            struct stat st;
            if ((lstat(copy, &st) != 0) || !S_ISDIR(st.st_mode)) {
                return err;
            }
        }
        else {
            return err;
        }
    }
    chmod(copy, stat->st_mode);
    chown(copy, stat->st_uid, stat->st_gid);
    // we dont need to set xattrs/time on the gufi directory those are in the db
    // the gufi directory structure is there only to walk, not to provide
    // information, the information is in the db

    return 0;
}

int shortpath(const char *name, char *nameout, char *endname) {
     char prefix[MAXPATH];
     char *pp;
     size_t i;
     int slashfound;

     *endname = 0;              // in case there's no '/'
     SNPRINTF(prefix, MAXPATH, "%s", name);
     i = strlen(prefix);
     pp = prefix+i;
     slashfound = 0;
     while (i > 0) {
       if (!strncmp(pp,"/",1)) {
          memset(pp, 0, 1);
          SNPRINTF(endname, MAXPATH, "%s", pp+1);
          slashfound = 1;
          break;
       }
       pp--;
       i--;
     }
     if (slashfound == 0) {
        SNPRINTF(endname, MAXPATH, "%s", name);
        memset(nameout, 0, 1);
     } else
        SNPRINTF(nameout, MAXPATH, "%s", prefix);
     return 0;
}

int SNPRINTF(char *str, size_t size, const char *format, ...) {
    va_list args;
    va_start(args, format);
    const int n = vsnprintf(str, size, format, args);
    va_end(args);
    if ((size_t) n >= size) {
        fprintf(stderr, "%s:%d Warning: Message "
                "was truncated to %d characters: %s\n",
                __FILE__, __LINE__, n, str);
    }
    return n;
}

/* Equivalent to snprintf printing only strings. Variadic arguments
   should be pairs of strings and their lengths (to try to prevent
   unnecessary calls to strlen). Make sure to typecast the lengths
   to size_t or weird bugs may occur */
size_t SNFORMAT_S(char *dst, const size_t dst_len, size_t count, ...) {
    size_t max_len = dst_len - 1;

    va_list args;
    va_start(args, count);
    for(size_t i = 0; i < count; i++) {
        const char *src = va_arg(args, char *);
        /* size_t does not work, but solution found at */
        /* https://stackoverflow.com/a/12864069/341683 */
        /* does not seem to fix it either */
        const size_t len = va_arg(args, unsigned int);
        const size_t copy_len = (len < max_len)?len:max_len;
        memcpy(dst, src, copy_len);
        dst += copy_len;
        max_len -= copy_len;
    }
    va_end(args);

    *dst = '\0';
    return dst_len - max_len - 1;
}

/*
 * Returns size of a dynamically sized struct work_packed.
 *
 * Size of base struct plus size of stored name plus NUL terminator.
 */
size_t struct_work_size(struct work *w) {
    return sizeof(*w) + w->name_len + 1;
}

/*
 * Allocates a new struct work on the heap with enough room to
 * fit the given `basename` with an optional `prefix`.
 *
 * Initializes the following fields:
 *   - name
 *   - name_len
 */
struct work *new_work_with_name(const char *prefix, const size_t prefix_len,
                                const char *basename, const size_t basename_len) {
    /* +1 for path separator */
    const size_t name_total = prefix_len + 1 + basename_len;

    struct work *w = calloc(1, sizeof(*w) + name_total + 1);
    w->name = (char *) &w[1];
    if (prefix_len == 0) {
        w->name_len = SNFORMAT_S(w->name, name_total + 1, 1,
                                 basename, basename_len);
    } else {
        w->name_len = SNFORMAT_S(w->name, name_total + 1, 3,
                                 prefix, prefix_len,
                                 "/", (size_t) 1,
                                 basename, basename_len);
    }

    // TODO: should this initialize basename length?
    // check how many callers use / need that...

    return w;
}

/* convert a mode to a human readable string */
char *modetostr(char *str, const size_t size, const mode_t mode)
{
    if (size < 11) {
        return NULL;
    }

    if (str) {
        SNPRINTF(str, size, "----------");
        if (S_ISDIR(mode))  str[0] = 'd';
        if (S_ISLNK(mode))  str[0] = 'l';
        if (mode & S_IRUSR) str[1] = 'r';
        if (mode & S_IWUSR) str[2] = 'w';
        if (mode & S_IXUSR) str[3] = 'x';
        if (mode & S_IRGRP) str[4] = 'r';
        if (mode & S_IWGRP) str[5] = 'w';
        if (mode & S_IXGRP) str[6] = 'x';
        if (mode & S_IROTH) str[7] = 'r';
        if (mode & S_IWOTH) str[8] = 'w';
        if (mode & S_IXOTH) str[9] = 'x';
    }

    return str;
}

static int loop_matches(const char c, const char *match, const size_t match_count) {
    for(size_t i = 0; i < match_count; i++) {
        if (match[i] == c) {
            return 1;
        }
    }

    return 0;
}

/* find the index of the first match, walking backwards */
size_t trailing_match_index(const char *str, size_t len,
                            const char *match, const size_t match_count) {
    if (!str) {
        return 0;
    }

    /* loop while not match */
    while (len && !loop_matches(str[len - 1], match, match_count)) {
        len--;
    }

    return len;
}

/* find the index of the first non-match, walking backwards */
size_t trailing_non_match_index(const char *str, size_t len,
                                const char *not_match, const size_t not_match_count) {
    if (!str) {
        return 0;
    }

    /* loop while match */
    while (len && loop_matches(str[len - 1], not_match, not_match_count)) {
        len--;
    }

    return len;
}

/*
 * find first slash before the basename a.k.a. get the dirname length
 *
 * /      -> /
 * /a/b/c -> /a/b/
 *
 * This function is meant to be called during input processing to get
 * the parent of each input directory. The input paths should not have
 * trailing slashes.
 *
 * This function allows for the index to be created in a directory
 * under the provided root, instead of directly in the provided
 * root. If the source directory is the root directory, this still
 * makes sense.
 *
 * toname = /home/search
 * source = /a/b/c
 * result = /home/search/c
 * source = /
 * result = /home/search/
 */
size_t dirname_len(const char *path, size_t len) {
    return trailing_match_index(path, len, "/", 1);
}

/* create a trie of directory names to skip from a file */
ssize_t setup_directory_skip(trie_t *skip, const char *filename) {
    if (!skip || !filename) {
        return -1;
    }

    /* add user defined directory names to skip */
    FILE *skipfile = fopen(filename, "r");
    if (!skipfile) {
        fprintf(stderr, "Error: Cannot open skip file \"%s\"\n", filename);
        return -1;
    }

    ssize_t count = 0;
    char line[MAXPATH];
    while (fgets(line, MAXPATH, skipfile) == line) {
        /* only keep first word */
        char name[MAXPATH];
        if (sscanf(line, "%s", name) != 1) {
            continue;
        }

        const size_t len = strlen(name);

        if (!trie_search(skip, name, len, NULL)) {
            trie_insert(skip, name, len, NULL, NULL);
            count++;
        }
    }

    fclose(skipfile);

    return count;
}

/* strstr/strtok replacement */
/* does not terminate on NULL character */
/* does not skip to the next non-empty column */
char *split(char *src, const char *delim, const size_t delim_len, const char *end) {
    if (!src || !delim || !delim_len || !end || (src > end)) {
        return NULL;
    }

    while (src < end) {
        for(size_t i = 0; i < delim_len; i++) {
            if (*src == delim[i]) {
                *src = '\x00';
                return src + 1;
            }
        }

        src++;
    }

    return NULL;
}

/* should work similarly to getline(3) */
ssize_t getline_fd(char **lineptr, size_t *n, int fd, off_t *offset, const size_t default_size) {
    if (!lineptr || !n || (fd < 0) || !offset || (default_size < 1)) {
        return -EINVAL;
    }

    size_t read = 0;
    ssize_t rc = 0;

    if (!*n) {
        *n = default_size;
    }

    if (!*lineptr) {
        void *new_alloc = realloc(*lineptr, *n);
        if (!new_alloc) {
            const int err = errno;
            fprintf(stderr, "Error: Could not realloc input address: %s (%d)\n", strerror(err), err);
            return -ENOMEM;
        }

        *lineptr = new_alloc;
    }

    int found = 0;
    while (!found) {
        if (read >= *n) {
            *n *= 2;
            void *new_alloc = realloc(*lineptr, *n);
            if (!new_alloc) {
                const int err = errno;
                fprintf(stderr, "Error: Could not realloc buffer: %s (%d)\n", strerror(err), err);
                return -ENOMEM;
            }

            *lineptr = new_alloc;
        }

        char *start = *lineptr + read;
        rc = pread(fd, start, *n - read, *offset + read);
        if (rc < 1) {
            break;
        }

        char *newline = memchr(start, '\n', rc);
        if (newline) {
            *newline = '\0';
            read += newline - start;
            found = 1;
        }
        else {
            read += rc;
        }
    }

    if (rc < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not pread: %s (%d)\n", strerror(err), err);
        return -err;
    }
    else if (rc == 0) {
        /* EOF */
        if ((read == 0) && (found == 0)) {
            return -EIO;
        }
    }

    /*
     * no need to possibly reallocate for NULL terminator
     *  - if buffer has newline, simply replace newline
     *  - if buffer does not have newline, must have hit EOF,
     *    and loop would have cycled back after reading and
     *    reallocated with more space before breaking
     */
    (*lineptr)[read] = '\0';

    *offset += read + found; /* remove newlne if it was read */

    return read;
}

#if defined(__APPLE__)

#include <copyfile.h>

ssize_t copyfd(int src_fd, off_t src_off,
               int dst_fd, off_t dst_off,
               size_t size) {
    (void) size;

    if (lseek(src_fd, src_off, SEEK_SET) != src_off) {
        return -1;
    }

    const off_t start = lseek(dst_fd, dst_off, SEEK_SET);
    if (start != dst_off) {
        return -1;
    }

    const int rc = fcopyfile(src_fd, dst_fd, 0, COPYFILE_DATA);
    if (rc < 0) {
        return rc;
    }

    const off_t end = lseek(dst_fd, 0, SEEK_CUR);
    if (end == (off_t) -1) {
        return -1;
    }

    return end - start;
}

#elif defined(__linux__)

#include <sys/sendfile.h>

ssize_t copyfd(int src_fd, off_t src_off,
               int dst_fd, off_t dst_off,
               size_t size) {
    if (lseek(dst_fd, dst_off, SEEK_SET) != dst_off) {
        return -1;
    }

    const ssize_t copied = size;

    while (size > 0) {
        /* sendfile(2): sendfile transfers at most 0x7ffff000 bytes */
        const ssize_t rc = sendfile(dst_fd, src_fd, &src_off, size);
        if (rc < 0) {
            return rc;
        }
        else if (rc == 0) {
            return copied - size;
        }

        size -= (size_t) rc;
    }

    return copied;
}

#else

ssize_t copyfd(int src_fd, off_t src_off,
               int dst_fd, off_t dst_off,
               size_t size) {
    #define buf_size 40960 /* size of empty db.db */
    char buf[buf_size];

    ssize_t copied = 0;
    while ((size_t) copied < size) {
        const size_t rem = size - copied;
        const ssize_t r = pread(src_fd, buf, (rem < buf_size)?rem:buf_size, src_off);
        if (r == 0) {
            break;
        }
        if (r < 0) {
            copied = -1;
            break;
        }

        src_off += r;

        ssize_t written = 0;
        while (written < r) {
            const ssize_t w = pwrite(dst_fd, buf + written, r - written, dst_off);
            if (w < 1) {
                copied = -1;
                break;
            }

            written += w;
            dst_off += w;
        }

        if (copied == -1) {
            break;
        }

        copied += written;
    }

    return copied;
}

#endif

/* replace root of actual path being walked with original user inputted root */
size_t present_user_path(const char *curr, size_t curr_len,
                         refstr_t *root_parent, const size_t root_basename_len, refstr_t *orig_root,
                         char *buf, size_t len) {
    const size_t prefix = root_parent->len + root_basename_len;

    /* curr + prefix comes with / prefixed, so no need for extra / */
    return SNFORMAT_S(buf, len, 2,
                      orig_root->data, orig_root->len,
                      curr + prefix, curr_len - prefix);
}

/* print errors, but keep going */
void set_metadata(const char *path, struct stat *st,
                  struct xattrs *xattrs) {
    /* Not checking arguments */
    if (chmod(path, st->st_mode) != 0) {
        const int err = errno;
        fprintf(stderr, "Error running chmod on %s: %s (%d)\n",
                path, strerror(err), err);
    }

    if (chown(path, st->st_uid, st->st_gid) != 0) {
        const int err = errno;
        fprintf(stderr, "Error running chown on %s: %s (%d)\n",
                path, strerror(err), err);
    }

    for(size_t i = 0; i < xattrs->count; i++) {
        struct xattr *xattr = &xattrs->pairs[i];
        if (SETXATTR(path, xattr->name, xattr->value, xattr->value_len) != 0) {
            const int err = errno;
            fprintf(stderr, "Error running setxattr on %s: %s (%d)\n",
                    path, strerror(err), err);
        }
    }

    /* set atime and mtime */
    struct utimbuf ut = {
        .actime = st->st_atime,
        .modtime = st->st_mtime,
    };
    if (utime(path, &ut) != 0) {
        const int err = errno;
        fprintf(stderr, "Error setting atime and mtime of %s: %s (%d)\n",
                path, strerror(err), err);
    }
}

void dump_memory_usage(void) {
    #if defined(DEBUG) && defined(__linux__)
    int fd = open("/proc/self/status", O_RDONLY);
    if (fd < 0) {
        perror("Could not open /proc/self/status");
        return;
    }

    char buf[4096] = { 0 };
    int rc = read(fd, buf, sizeof buf - 1);
    if (rc < 0) {
        perror("Could not read /proc/self/status");
        return;
    }

    const char *field = "VmHWM";
    char *p = buf;
    p = strtok(p, "\n");
    while (p && strncmp(p, field, strlen(field)))
        p = strtok(NULL, "\n");

    if (p)
        printf("%s\n", p);
    #endif
}

ssize_t write_size(const int fd, const void *data, const size_t size) {
    /* Not checking arguments */
    char *curr = (char *) data;
    size_t written = 0;
    while (written < size) {
        const ssize_t w = write(fd, curr, size - written);

        if (w < 0) {
            const int err = errno;
            fprintf(stderr, "Error: Failed to write to file descriptor %d: %s (%d)\n",
                    fd, strerror(err), err);
            return -1;
        }
        else if (w == 0) {
            break;
        }

        curr += w;
        written += w;
    }

    return written;
}

ssize_t read_size(const int fd, void *buf, const size_t size) {
    /* Not checking arguments */
    char *curr = (char *) buf;
    size_t got = 0;
    while (got < size) {
        const ssize_t r = read(fd, curr, size - got);

        if (r < 0) {
            const int err = errno;
            fprintf(stderr, "Error: Failed to read from file descriptor %d: %s (%d)\n",
                    fd, strerror(err), err);
            return -1;
        }
        else if (r == 0) {
            /* EOF */
            break;
        }

        curr += r;
        got += r;
    }

    return got;
}

/*
 * Get a directory file descriptor from a given DIR *, infallibly.
 *
 * This is just a wrapper around libc's dirfd() that panics if an error is reported, since
 * an error in dirfd() always indicates a bug in GUFI.
 */
int gufi_dirfd(DIR *d) {
    int d_fd = dirfd(d);
    if (d_fd < 0) {
        /*
         * We should never get here. glibc's dirfd(3) never return errors, and
         * Apple's libc only returns an error if the DIR * is invalid, which would
         * indicate a bug in GUFI.
         */
        fprintf(stderr, "BUG: dirfd(3) failed: errno = %d\n", errno);
        exit(1);
    }

    return d_fd;
}
