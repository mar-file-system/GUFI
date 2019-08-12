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

-----
NOTE:
-----

GUFI uses the C-Thread-Pool library.  The original version, written by
Johan Hanssen Seferidis, is found at
https://github.com/Pithikos/C-Thread-Pool/blob/master/LICENSE, and is
released under the MIT License.  LANS, LLC added functionality to the
original work.  The original work, plus LANS, LLC added functionality is
found at https://github.com/jti-lanl/C-Thread-Pool, also under the MIT
License.  The MIT License can be found at
https://opensource.org/licenses/MIT.


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
#include <pthread.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "C-Thread-Pool/thpool.h"
#include <sqlite3.h>

#include "config.h"
#include "bf.h"

#define SNPRINTF(STR, N, FMT, ...)                                      \
    do {                                                                \
        const int n = snprintf(STR, N, FMT, ##__VA_ARGS__);             \
        if (n >= N) {                                                   \
            fprintf(stderr, "%s:%d Warning: Message %s "                \
                    "was truncated to %d characters: \"" FMT "\"\n",    \
                    __FILE__, __LINE__, FMT, n, ##__VA_ARGS__);         \
        }                                                               \
    } while (0)

/* this block is for the triell */
/* we think this should be 10 since we are just using chars 0-9 but 10 doesnt work for some reason */
#define CHAR_SIZE 24
//#define CHAR_SIZE 12
struct Trie
{
    int isLeaf;    // 1 when node is a leaf node
    struct Trie* character[CHAR_SIZE];
};

struct Trie* getNewTrieNode();
void insertll(struct Trie* *head, char* str);
int searchll(struct Trie* head, char* str);
int haveChildren(struct Trie* curr);
int deletionll(struct Trie* *curr, char* str);
void cleanup(struct Trie *head);

extern threadpool mythpool;

// global variable to hold per thread state goes here
struct globalthreadstate {
   FILE*    outfd[MAXPTHREAD];
   sqlite3* outdbd[MAXPTHREAD];
};
extern struct globalthreadstate gts;

extern struct sum sumout;

int printits(struct work *pwork,int ptid);

int pullxattrs( const char *name, char *bufx);

int zeroit(struct sum *summary);

int sumit (struct sum *summary,struct work *pwork);

int tsumit (struct sum *sumin,struct sum *smout);

// given a possibly-multi-level path of directories (final component is
// also a dir), create the parent dirs all the way down.
//
int mkpath(char* path, mode_t mode);

int dupdir(char* path, struct stat * stat);

int incrthread();

int decrthread();

int getqent();

int pushdir( void  * qqwork);

int gettid();

int shortpath(const char *name, char *nameout, char *endname);

int printit(const char *name, const struct stat *status, char *type, char *linkname, int xattrs, char * xattr,int printing, long long pinode);

// NOTE: returns void, not void*, because threadpool threads
//       do not return values outside the API.
typedef void(DirFunc)(void*);

int processdirs(DirFunc dir_fn);

// Function used in processdir to decend into subdirectories.
// The callback function is used to modify the qwork before
// it is pushed onto the queue. The callback function should
// return 0 if there were no errors. Non-zero values results
// the qwork not being pushed onto the queue.
size_t descend(struct work *passmywork, DIR *dir,
               const size_t max_level);

#if defined(DEBUG) || BENCHMARK

#include <time.h>

long double elapsed(const struct timespec *start, const struct timespec *end);

#endif

#endif
