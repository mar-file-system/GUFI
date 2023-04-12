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



#ifndef UTILS_H
#define UTILS_H

#include <dirent.h>
#include <stdio.h>
#include <sys/types.h>

#include <sqlite3.h>

#include "QueuePerThreadPool.h"
#include "bf.h"
#include "config.h"
#include "trie.h"
#include "xattrs.h"

/* Wrapper around snprintf to catch issues and print them to stderr */
int SNPRINTF(char *str, size_t size, const char *format, ...);

/* Equivalent to snprintf printing only strings. Variadic arguments
   should be pairs of strings and their lengths (to try to prevent
   unnecessary calls to strlen). Make sure to typecast the lengths
   to size_t or weird bugs may occur */
size_t SNFORMAT_S(char *dst, const size_t dst_len, size_t count, ...);

int printits(struct input *in, struct work *pwork, struct entry_data *data, FILE *out);

int zeroit(struct sum *summary);

int sumit(struct sum *summary, struct entry_data *data);

int tsumit (struct sum *sumin, struct sum *smout);

// given a possibly-multi-level path of directories (final component is
// also a dir), create the parent dirs all the way down.
//
int mkpath(char*path, mode_t mode, uid_t uid, gid_t gid);

int dupdir(const char *path, struct stat *stat);

int shortpath(const char *name, char *nameout, char *endname);

typedef int (*processnondir_f)(struct work *nondir, struct entry_data *ed, void *nondir_args);

/*
 * Push the subdirectories in the current directory onto the queue
 * and process non directories using a user provided function
 */
int descend(QPTPool_t *ctx, const size_t id, void *args,
            struct input *in, struct work *work, ino_t inode,
            DIR *dir, trie_t *skip, const int skip_db,
            const int stat_entries,  QPTPoolFunc_t processdir,
            processnondir_f processnondir, void *nondir_args,
            size_t *dir_count, size_t *dirs_insitu, size_t *nondir_count, size_t *nondirs_processed);

/* convert a mode to a human readable string */
char *modetostr(char *str, const size_t size, const mode_t mode);

/* find the index of the first match, walking backwards */
size_t trailing_match_index(const char *str, size_t len,
                            const char *match, const size_t match_count);

/* find the index of the first non-match, walking backwards */
size_t trailing_non_match_index(const char *str, size_t len,
                                const char *not_match, const size_t not_match_count);

/*
 * convenience function to find first slash before the basename
 *
 * used for processing input paths that have been run through realpath
 */
size_t dirname_len(const char *path, size_t len);

int setup_directory_skip(const char *filename, trie_t **skip);

/* strstr/strtok replacement */
char *split(char *src, const char *delim, const size_t delim_len, const char *end);

#endif
