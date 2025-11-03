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
#if HAVE_STATX
#include <sys/sysmacros.h>
#endif
#include <unistd.h>
#include <utime.h>

#include "external.h"
#include "str.h"
#include "utils.h"

uint64_t get_queue_limit(const uint64_t target_memory_footprint, const uint64_t nthreads) {
    /* not set */
    if (target_memory_footprint == 0) {
        return 0;
    }

    /* set, so queue_limit should be at minimum 1 */
    return max(target_memory_footprint / sizeof(struct work) / nthreads, 1);
}

int zeroit(struct sum *summary) {
    summary->totfiles = 0;
    summary->totlinks = 0;
    summary->minuid = LLONG_MAX;
    summary->maxuid = LLONG_MIN;
    summary->mingid = LLONG_MAX;
    summary->maxgid = LLONG_MIN;
    summary->minsize = LLONG_MAX;
    summary->maxsize = LLONG_MIN;
    summary->totzero = 0;
    summary->totltk = 0;
    summary->totmtk = 0;
    summary->totltm = 0;
    summary->totmtm = 0;
    summary->totmtg = 0;
    summary->totmtt = 0;
    summary->totsize = 0;
    summary->minctime = LLONG_MAX;
    summary->maxctime = LLONG_MIN;
    summary->totctime = 0;
    summary->minmtime = LLONG_MAX;
    summary->maxmtime = LLONG_MIN;
    summary->totmtime = 0;
    summary->minatime = LLONG_MAX;
    summary->maxatime = LLONG_MIN;
    summary->totatime = 0;
    summary->minblocks = LLONG_MAX;
    summary->maxblocks = LLONG_MIN;
    summary->totblocks = 0;
    summary->totxattr = 0;
    summary->totsubdirs = 0;
    summary->maxsubdirfiles = LLONG_MIN;
    summary->maxsubdirlinks = LLONG_MIN;
    summary->maxsubdirsize = LLONG_MIN;
    summary->mincrtime = LLONG_MAX;
    summary->maxcrtime = LLONG_MIN;
    summary->totcrtime = 0;
    summary->minossint1 = LLONG_MAX;
    summary->maxossint1 = LLONG_MIN;
    summary->totossint1 = 0;
    summary->minossint2 = LLONG_MAX;
    summary->maxossint2 = LLONG_MIN;
    summary->totossint2 = 0;
    summary->minossint3 = LLONG_MAX;
    summary->maxossint3 = LLONG_MIN;
    summary->totossint3 = 0;
    summary->minossint4 = LLONG_MAX;
    summary->maxossint4 = LLONG_MIN;
    summary->totossint4 = 0;
    summary->totextdbs = 0;
    return 0;
}

int sumit(struct sum *summary, struct work *work, struct entry_data *ed) {
    if (ed->type == 'f') {
        summary->totfiles++;
        MIN_ASSIGN_LHS(summary->minsize, work->statuso.st_size);
        MAX_ASSIGN_LHS(summary->maxsize, work->statuso.st_size);
        if (work->statuso.st_size == 0)             summary->totzero++;
        if (work->statuso.st_size <= 1024)          summary->totltk++;
        if (work->statuso.st_size >  1024)          summary->totmtk++;
        if (work->statuso.st_size <= 1048576)       summary->totltm++;
        if (work->statuso.st_size >  1048576)       summary->totmtm++;
        if (work->statuso.st_size >  1073741824)    summary->totmtg++;
        if (work->statuso.st_size >  1099511627776) summary->totmtt++;
        summary->totsize += work->statuso.st_size;

        MIN_ASSIGN_LHS(summary->minblocks, work->statuso.st_blocks);
        MAX_ASSIGN_LHS(summary->maxblocks, work->statuso.st_blocks);
        summary->totblocks += work->statuso.st_blocks;
    }
    if (ed->type == 'l') {
        summary->totlinks++;
    }

    MIN_ASSIGN_LHS(summary->minuid, work->statuso.st_uid);
    MAX_ASSIGN_LHS(summary->maxuid, work->statuso.st_uid);
    MIN_ASSIGN_LHS(summary->mingid, work->statuso.st_gid);
    MAX_ASSIGN_LHS(summary->maxgid, work->statuso.st_gid);

    MIN_ASSIGN_LHS(summary->minctime, work->statuso.st_ctime);
    MAX_ASSIGN_LHS(summary->maxctime, work->statuso.st_ctime);
    summary->totctime += work->statuso.st_ctime;

    MIN_ASSIGN_LHS(summary->minmtime, work->statuso.st_mtime);
    MAX_ASSIGN_LHS(summary->maxmtime, work->statuso.st_mtime);
    summary->totmtime += work->statuso.st_mtime;

    MIN_ASSIGN_LHS(summary->minatime, work->statuso.st_atime);
    MAX_ASSIGN_LHS(summary->maxatime, work->statuso.st_atime);
    summary->totatime += work->statuso.st_atime;

    MIN_ASSIGN_LHS(summary->mincrtime, work->crtime);
    MAX_ASSIGN_LHS(summary->maxcrtime, work->crtime);
    #if HAVE_STATX
    summary->totcrtime += work->crtime;
    #endif

    MIN_ASSIGN_LHS(summary->minossint1, ed->ossint1);
    MAX_ASSIGN_LHS(summary->maxossint1, ed->ossint1);
    summary->totossint1 += ed->ossint1;

    MIN_ASSIGN_LHS(summary->minossint2, ed->ossint2);
    MAX_ASSIGN_LHS(summary->maxossint2, ed->ossint2);
    summary->totossint2 += ed->ossint2;

    MIN_ASSIGN_LHS(summary->minossint3, ed->ossint3);
    MAX_ASSIGN_LHS(summary->maxossint3, ed->ossint3);
    summary->totossint3 += ed->ossint3;

    MIN_ASSIGN_LHS(summary->minossint4, ed->ossint4);
    MAX_ASSIGN_LHS(summary->maxossint4, ed->ossint4);
    summary->totossint4 += ed->ossint4;

    summary->totxattr += ed->xattrs.count;

    return 0;
}

int tsumit(struct sum *sumin, struct sum *smout) {
    /* count self as subdirectory because can't tell from here where treesummary descent started */
    smout->totsubdirs++;

    /* if sumin totsubdirs is > 0 we are summing a treesummary not a dirsummary */
    if (sumin->totsubdirs > 0) {
        smout->totsubdirs += sumin->totsubdirs;
        MAX_ASSIGN_LHS(smout->maxsubdirfiles, sumin->maxsubdirfiles);
        MAX_ASSIGN_LHS(smout->maxsubdirlinks, sumin->maxsubdirlinks);
        MAX_ASSIGN_LHS(smout->maxsubdirsize,  sumin->maxsubdirsize);
    } else {
        MAX_ASSIGN_LHS(smout->maxsubdirfiles, sumin->totfiles);
        MAX_ASSIGN_LHS(smout->maxsubdirlinks, sumin->totlinks);
        MAX_ASSIGN_LHS(smout->maxsubdirsize,  sumin->totsize);
    }

    smout->totfiles += sumin->totfiles;
    smout->totlinks += sumin->totlinks;

    if (sumin->totfiles > 0) {
        /* only set these mins and maxes if there are files in the directory
           otherwise mins are all zero */
        MIN_ASSIGN_LHS(smout->minuid, sumin->minuid);
        MAX_ASSIGN_LHS(smout->maxuid, sumin->maxuid);
        MIN_ASSIGN_LHS(smout->mingid, sumin->mingid);
        MAX_ASSIGN_LHS(smout->maxgid, sumin->maxgid);

        MIN_ASSIGN_LHS(smout->minsize, sumin->minsize);
        MAX_ASSIGN_LHS(smout->maxsize, sumin->maxsize);

        MIN_ASSIGN_LHS(smout->minblocks, sumin->minblocks);
        MAX_ASSIGN_LHS(smout->maxblocks, sumin->maxblocks);

        MIN_ASSIGN_LHS(smout->minctime, sumin->minctime);
        MAX_ASSIGN_LHS(smout->maxctime, sumin->maxctime);
        smout->totctime += sumin->totctime;

        MIN_ASSIGN_LHS(smout->minmtime, sumin->minmtime);
        MAX_ASSIGN_LHS(smout->maxmtime, sumin->maxmtime);
        smout->totmtime += sumin->totmtime;

        MIN_ASSIGN_LHS(smout->minatime, sumin->minatime);
        MAX_ASSIGN_LHS(smout->maxatime, sumin->maxatime);
        smout->totatime += sumin->totatime;

        MIN_ASSIGN_LHS(smout->mincrtime, sumin->mincrtime);
        MAX_ASSIGN_LHS(smout->maxcrtime, sumin->maxcrtime);
        smout->totcrtime += sumin->totcrtime;

        MIN_ASSIGN_LHS(smout->minossint1, sumin->minossint1);
        MAX_ASSIGN_LHS(smout->maxossint1, sumin->maxossint1);
        MIN_ASSIGN_LHS(smout->minossint2, sumin->minossint2);
        MAX_ASSIGN_LHS(smout->maxossint2, sumin->maxossint2);
        MIN_ASSIGN_LHS(smout->minossint3, sumin->minossint3);
        MAX_ASSIGN_LHS(smout->maxossint3, sumin->maxossint3);
        MIN_ASSIGN_LHS(smout->minossint4, sumin->minossint4);
        MAX_ASSIGN_LHS(smout->maxossint4, sumin->maxossint4);
    }

    smout->totblocks    += sumin->totblocks;
    smout->totzero      += sumin->totzero;
    smout->totltk       += sumin->totltk;
    smout->totmtk       += sumin->totmtk;
    smout->totltm       += sumin->totltm;
    smout->totmtm       += sumin->totmtm;
    smout->totmtg       += sumin->totmtg;
    smout->totmtt       += sumin->totmtt;
    smout->totsize      += sumin->totsize;
    smout->totxattr     += sumin->totxattr;
    smout->totossint1   += sumin->totossint1;
    smout->totossint2   += sumin->totossint2;
    smout->totossint3   += sumin->totossint3;
    smout->totossint4   += sumin->totossint4;

    smout->totextdbs    += sumin->totextdbs;

    return 0;
}

// given a possibly-multi-level path of directories (final component is
// also a dir), create the parent dirs all the way down.
//
int mkpath(const char *path, const mode_t mode, const uid_t uid, const gid_t gid) {
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

int dupdir(const char *path, const mode_t mode, const uid_t uid, const gid_t gid) {
    // the writer must be able to create the index files into this directory so or in S_IWRITE
    if (mkdir(path, mode) != 0) {
        const int err = errno;
        if (err == ENOENT) {
            mkpath(path, mode, uid, gid);
        }
        else if (err == EEXIST) {
            struct stat st;
            if ((lstat(path, &st) != 0) || !S_ISDIR(st.st_mode)) {
                return err;
            }
        }
        else {
            return err;
        }
    }
    chmod(path, mode);
    chown(path, uid, gid);
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

    char *line = NULL;
    size_t n = 0;
    ssize_t len = 0;
    while ((len = getline(&line, &n, skipfile)) > -1) {
        len = trailing_non_match_index(line, len, "\r\n", 2);
        if (len == 0) {
            continue;
        }

        if (!trie_search(skip, line, len, NULL)) {
            trie_insert(skip, line, len, NULL, NULL);
            count++;
        }
    }
    free(line);

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
static ssize_t getline_fd(char **lineptr, size_t *n, int fd, off_t *offset, const size_t default_size) {
    if (!lineptr || !n || (fd < 0) || (default_size < 1)) {
        return -EINVAL;
    }

    size_t r = 0;
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
        if (r >= *n) {
            *n *= 2;
            void *new_alloc = realloc(*lineptr, *n);
            if (!new_alloc) {
                const int err = errno;
                fprintf(stderr, "Error: Could not realloc buffer: %s (%d)\n", strerror(err), err);
                return -ENOMEM;
            }

            *lineptr = new_alloc;
        }

        char *start = *lineptr + r;
        if (offset) {
            rc = pread(fd, start, *n - r, *offset + r);
        }
        else {
            rc = read(fd, start, 1);
        }
        if (rc < 1) {
            break;
        }

        char *newline = memchr(start, '\n', rc);
        if (newline) {
            *newline = '\0';
            r += newline - start;
            found = 1;
        }
        else {
            r += rc;
        }
    }

    if (rc < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not %s: %s (%d)\n", offset?"pread":"read", strerror(err), err);
        return -err;
    }
    else if (rc == 0) {
        /* EOF */
        if ((r == 0) && (found == 0)) {
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
    (*lineptr)[r] = '\0';

    if (offset) {
        *offset += r + found; /* remove newlne if it was read */
    }

    return r;
}

ssize_t getline_fd_seekable(char **lineptr, size_t *n, int fd, off_t *offset, const size_t default_size) {
    if (!offset) {
        return -EINVAL;
    }

    return getline_fd(lineptr, n, fd, offset, default_size);
}

ssize_t getline_fd_stream(char **lineptr, size_t *n, int fd, const size_t default_size) {
    return getline_fd(lineptr, n, fd, NULL, default_size);
}

#if defined(__linux__)

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
    #define buf_size 49152 /* size of empty db.db */
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

void dump_memory_usage(FILE *out) {
    #if defined(DEBUG) && defined(__linux__)
    int fd = open("/proc/self/status", O_RDONLY);
    if (fd < 0) {
        perror("Could not open /proc/self/status");
        return;
    }

    char buf[4096] = { 0 };
    const int rc = read(fd, buf, sizeof buf - 1);
    const int err = errno;
    close(fd);
    if (rc < 0) {
        fprintf(stderr, "Error: Could not read /proc/self/status: %s (%d)\n",
                strerror(err), err);
        return;
    }

    const char *field = "VmHWM";
    char *p = buf;
    p = strtok(p, "\n");
    while (p && strncmp(p, field, strlen(field)))
        p = strtok(NULL, "\n");

    if (p) {
        fprintf(out, "%s\n", p);
    }
    #else
    (void) out;
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

#if HAVE_STATX
void statx_to_work(struct statx *stx, struct work *work) {
    struct stat *st = &work->statuso;
    st->st_dev      = (dev_t)     makedev(stx->stx_dev_major, stx->stx_dev_minor);
    st->st_ino      = (ino_t)     stx->stx_ino;
    st->st_mode     = (mode_t)    stx->stx_mode;
    st->st_nlink    = (nlink_t)   stx->stx_nlink;
    st->st_uid      = (uid_t)     stx->stx_uid;
    st->st_gid      = (gid_t)     stx->stx_gid;
    st->st_rdev     = (dev_t)     makedev(stx->stx_rdev_major, stx->stx_rdev_minor);
    st->st_size     = (off_t)     stx->stx_size;
    st->st_blksize  = (blksize_t) stx->stx_blksize;
    st->st_blocks   = (blkcnt_t)  stx->stx_blocks;
    st->st_atime    = (time_t)    stx->stx_atime.tv_sec;
    st->st_mtime    = (time_t)    stx->stx_mtime.tv_sec;
    st->st_ctime    = (time_t)    stx->stx_ctime.tv_sec;
    work->crtime    = (time_t)    stx->stx_btime.tv_sec;
}
#endif

/* try to call statx if available, otherwise, call lstat */
int lstat_wrapper(struct work *work) {
    /* don't duplicate work */
    if (work->stat_called != STAT_NOT_CALLED) {
        return 0;
    }

    #if HAVE_STATX
    struct statx stx;
    if (statx(AT_FDCWD, work->name,
              AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC,
              STATX_ALL, &stx) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not statx \"%s\": %s (%d)\n",
                work->name, strerror(err), err);
        return 1;
    }

    statx_to_work(&stx, work);

    work->stat_called = STATX_CALLED;
    #else
    if (lstat(work->name, &work->statuso) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not lstat \"%s\": %s (%d)\n",
                work->name, strerror(err), err);
        return 1;
    }

    work->crtime = 0;

    work->stat_called = NOT_STATX_CALLED;
    #endif

    return 0;
}

/* used by gufi_dir2index and gufi_dir2trace */
int fstatat_wrapper(struct work *entry, struct entry_data *ed) {
    /* don't duplicate work */
    if (entry->stat_called != STAT_NOT_CALLED) {
        return 0;
    }

    const char *basename = entry->name + entry->name_len - entry->basename_len;

    #if HAVE_STATX
    struct statx stx;
    if (statx(ed->parent_fd, basename,
              AT_SYMLINK_NOFOLLOW | AT_STATX_DONT_SYNC,
              STATX_ALL, &stx) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not statx \"%s\": %s (%d)\n",
                entry->name, strerror(err), err);
        return 1;
    }

    statx_to_work(&stx, entry);

    entry->stat_called = STATX_CALLED;
    #else
    if (fstatat(ed->parent_fd, basename, &entry->statuso, AT_SYMLINK_NOFOLLOW) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not fstatat \"%s\": %s (%d)\n",
                entry->name, strerror(err), err);
        return 1;
    }

    entry->crtime = 0;

    entry->stat_called = NOT_STATX_CALLED;
    #endif

    return 0;
}

/* not the same as !doing_partial_walk */
int bad_partial_walk(struct input *in, const size_t root_count) {
    if ((in->path_list.data && in->path_list.len) &&
        (root_count > 1)) {
        fprintf(stderr, "Error: When -D is passed in, only one root directory may be specified\n");
        return 1;
    }
    return 0;
}

int doing_partial_walk(struct input *in, const size_t root_count) {
    (void) root_count;
    return ((in->path_list.data && in->path_list.len) /* && */
            /* (root_count < 2) */
        );
}

/*
 * attach directory paths directly to the root path and
 * run starting at --min-level instead of walking to --min-level first
 */
ssize_t process_path_list(struct input *in, struct work *root,
                          QPTPool_t *ctx, QPTPool_f func) {
    FILE *file = fopen(in->path_list.data, "r");
    if (!file) {
        const int err = errno;
        fprintf(stderr, "could not open directory list file \"%s\": %s (%d)\n",
                in->path_list.data, strerror(err), err);
        return -1;
    }

    ssize_t enqueue_count = 0;

    char *line = NULL;
    size_t n = 0;
    ssize_t got = 0;
    while ((got = getline(&line, &n, file)) != -1) {
        /* remove trailing CRLF */
        const size_t len = trailing_non_match_index(line, got, "\r\n", 2);

        if (len == 0) {
            continue;
        }

        /* if min-level == 0, do not prefix root */
        struct work *subtree_root = new_work_with_name(in->min_level?root->name:NULL,
                                                       in->min_level?root->name_len:0,
                                                       line, len);

        /* directory symlinks are not allowed under the root */
        if (lstat_wrapper(subtree_root) != 0) {
            free(subtree_root);
            continue;
        }

        /* check that the subtree root is a directory */
        if (!S_ISDIR(subtree_root->statuso.st_mode)) {
            line[len] = '\0';
            fprintf(stderr, "Error: Subtree root is not a directory \"%s\"\n",
                    line);
            free(subtree_root);
            continue;
        }

        subtree_root->orig_root = root->orig_root;

        /* parent of the input path, not the subtree root */
        subtree_root->root_parent = root->root_parent;

        /* remove trailing slashes (+ 1 to keep at least 1 character) */
        subtree_root->basename_len = subtree_root->name_len - (trailing_match_index(subtree_root->name + 1, subtree_root->name_len - 1, "/", 1) + 1);

        /* go directly to --min-level */
        subtree_root->level = in->min_level;

        QPTPool_enqueue(ctx, 0, func, subtree_root);

        enqueue_count++;
    }

    free(line);
    fclose(file);
    free(root);

    return enqueue_count;
}

int write_with_resize(char **buf, size_t *size, size_t *offset,
                      const char *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    const int written = vsnprintf(*buf + *offset, *size - *offset, fmt, args);
    va_end(args);

    /* not enough space */
    if ((size_t) written >= (*size - *offset)) {
        const size_t new_size = max(*size * 2, *size + written) + 1;
        void *ptr = realloc(*buf, new_size);
        if (!ptr) {
            const int err = errno;
            fprintf(stderr, "Error: Failed to reallocate space for stanza: %s (%d)\n",
                    strerror(err), err);
            return 1;
        }

        *buf = ptr;
        *size = new_size;

        /* try again */
        va_start(args, fmt);
        const int write_again = vsnprintf(*buf + *offset, *size - *offset, fmt, args);
        va_end(args);

        *offset += write_again;
    }
    else {
        *offset += written;
    }

    return 0;
}
