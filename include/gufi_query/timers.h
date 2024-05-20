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



#ifndef GUFI_QUERY_TIMERS_H
#define GUFI_QUERY_TIMERS_H

#include <pthread.h>
#include <stdint.h>
#include <time.h>

#include "OutputBuffers.h"
#include "SinglyLinkedList.h"
#include "debug.h"

/* ************************************************************************* */
/* global/pool variables */
typedef struct total_time {
    pthread_mutex_t mutex;

    uint64_t lstat;
    uint64_t opendir;
    uint64_t attachdb;
    uint64_t lstat_db;
    uint64_t addqueryfuncs;
    uint64_t attach_external;
    uint64_t xattrprep;
    uint64_t get_rollupscore;
    uint64_t sqltsumcheck;
    uint64_t sqltsum;
    uint64_t descend;
    uint64_t check_args;
    uint64_t level;
    uint64_t level_branch;
    uint64_t while_branch;
    uint64_t readdir;
    uint64_t readdir_branch;
    uint64_t strncmp;
    uint64_t strncmp_branch;
    uint64_t snprintf;
    uint64_t isdir;
    uint64_t isdir_branch;
    uint64_t access;
    uint64_t set;
    uint64_t clone;
    uint64_t pushdir;
    uint64_t sqlsum;
    uint64_t sqlent;
    uint64_t xattrdone;
    uint64_t detach_external;
    uint64_t detachdb;
    uint64_t closedir;
    uint64_t utime;
    uint64_t free_work;
    uint64_t output_timestamps;
} total_time_t;

void total_time_init(total_time_t *tt);
uint64_t total_time_sum(total_time_t *tt);
void total_time_destroy(total_time_t *tt);

/* ************************************************************************* */
/* descend timestamp types (used to index into array) */
enum {
    dts_within_descend = 0,
    dts_check_args,
    dts_level_cmp,
    dts_level_branch,
    dts_while_branch,
    dts_readdir_call,
    dts_readdir_branch,
    dts_strncmp_call,
    dts_strncmp_branch,
    dts_snprintf_call,
    dts_isdir_cmp,
    dts_isdir_branch,
    dts_access_call,
    dts_set,
    dts_make_clone,
    dts_pushdir,

    dts_max
};

/*
 * this function is a macro to allow for a variable with
 * the name of name to be created in the calling scope
 *
 * name should be one of the dts_* enums, without the dts_
 */
#if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
#define descend_timestamp_start(dts, name)                      \
    struct start_end *name = malloc(sizeof(struct start_end));  \
    clock_gettime(CLOCK_MONOTONIC, &(name)->start);             \
                                                                \
    do {                                                        \
        /* get list where new timestamp will be placed */       \
        sll_t *dst = &((dts)[dts_##name]);                      \
                                                                \
        /* keep track of new timestamp */                       \
        sll_push(dst, name);                                    \
    } while (0)

#define descend_timestamp_end(name)                             \
    clock_gettime(CLOCK_MONOTONIC, &(name)->end);
#else
#define descend_timestamp_start(dts, name)
#define descend_timestamp_end(name)
#endif

/* ************************************************************************* */
/* thread timestamp types (used to index into array) */
enum {
    tts_lstat_call = 0,
    tts_opendir_call,
    tts_lstat_db_call,
    tts_attachdb_call,
    tts_addqueryfuncs_call,
    tts_attach_external,
    tts_xattrprep_call,
    tts_sqltsumcheck,
    tts_sqltsum,
    tts_get_rollupscore_call,
    tts_descend_call,
    tts_sqlsum,
    tts_sqlent,
    tts_xattrdone_call,
    tts_detach_external,
    tts_detachdb_call,
    tts_closedir_call,
    tts_utime_call,
    tts_free_work,
    tts_output_timestamps,

    tts_max
};

/* thread variables */
typedef struct timestamps {
    struct start_end tts[tts_max]; /* thread timestamps */
    sll_t dts[dts_max];            /* descend timestamps */
} timestamps_t;

void timestamps_init(timestamps_t *ts, struct timespec *zero);

#if defined(DEBUG) && (defined(CUMULATIVE_TIMES) || defined(PER_THREAD_STATS))
#define thread_timestamp_start(tts, name)           \
    struct start_end *name = &tts[tts_##name];      \
    clock_gettime(CLOCK_MONOTONIC, &(name)->start);

#define thread_timestamp_end(name)                  \
    clock_gettime(CLOCK_MONOTONIC, &(name)->end);
#else
#define thread_timestamp_start(tts, name)
#define thread_timestamp_end(name)
#endif

void timestamps_print(struct OutputBuffers *obs, const size_t id,
                      timestamps_t *ts, void *dir, void *db);
void timestamps_sum(total_time_t *tt, timestamps_t *ts);
void timestamps_destroy(timestamps_t *ts);

#endif
