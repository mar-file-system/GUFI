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



#include <stdlib.h>
#include <string.h>

#include "debug.h"
#include "gufi_query/timers.h"

void total_time_init(total_time_t *tt) {
    memset(tt, 0, sizeof(*tt));
    pthread_mutex_init(&tt->mutex, NULL);
}

uint64_t total_time_sum(total_time_t *tt) {
    return (tt->lstat + tt->opendir + tt->lstat_db +
            tt->attachdb + tt->addqueryfuncs +
            tt->xattrprep + tt->get_rollupscore +
            tt->sqltsumcheck + tt->sqltsum + tt->descend +
            tt->check_args + tt->level + tt->level_branch +
            tt->while_branch + tt->readdir +
            tt->readdir_branch + tt->strncmp +
            tt->strncmp_branch + tt->snprintf + tt->isdir +
            tt->isdir_branch + tt->access + tt->set +
            tt->clone + tt->pushdir + tt->sqlsum +
            tt->sqlent + tt->xattrdone + tt->detachdb +
            tt->closedir + tt->utime + tt->free_work +
            tt->output_timestamps);
}

void total_time_destroy(total_time_t *tt) {
    pthread_mutex_destroy(&tt->mutex);
}

void timestamps_init(timestamps_t *ts, struct timespec *zero) {
    for(int i = 0; i < tts_max; i++) {
        memcpy(&ts->tts[i].start, zero, sizeof(*zero));
        memcpy(&ts->tts[i].end,   zero, sizeof(*zero));
    }

    /*
     * no need to initialize descend timers because
     * all starts and ends are guaranteed to be set
     */
    for(int i = 0; i < dts_max; i++) {
        sll_init(&ts->dts[i]);
    }
}

/* print all descend timestamps of one type */
static void print_descend(struct OutputBuffers *obufs, const size_t id,
                          const char *name, sll_t *sll) {
    sll_loop(sll, node) {
        struct start_end *timestamp = sll_node_data(node);
        print_timer(obufs, id, name, timestamp);
    }
}

void timestamps_print(struct OutputBuffers *obs, const size_t id,
                      timestamps_t *ts, void *dir, void *db) {
    thread_timestamp_start(ts->tts, output_timestamps);
    print_timer          (obs, id, "lstat",              &ts->tts[tts_lstat_call]);
    print_timer          (obs, id, "opendir",            &ts->tts[tts_opendir_call]);
    if (dir) {
        print_timer      (obs, id, "lstat_db",           &ts->tts[tts_lstat_db_call]);
        print_timer      (obs, id, "attachdb",           &ts->tts[tts_attachdb_call]);
        if (db) {
            print_timer  (obs, id, "addqueryfuncs",      &ts->tts[tts_addqueryfuncs_call]);
            print_timer  (obs, id, "xattrprep",          &ts->tts[tts_xattrprep_call]);
            print_timer  (obs, id, "sqltsumcheck",       &ts->tts[tts_sqltsumcheck]);
            print_timer  (obs, id, "sqltsum",            &ts->tts[tts_sqltsum]);
            print_timer  (obs, id, "get_rollupscore",    &ts->tts[tts_get_rollupscore_call]);
            print_timer  (obs, id, "descend",            &ts->tts[tts_descend_call]);
            print_descend(obs, id, "within_descend",     &ts->dts[dts_within_descend]);
            print_descend(obs, id, "check_args",         &ts->dts[dts_check_args]);
            print_descend(obs, id, "level",              &ts->dts[dts_level_cmp]);
            print_descend(obs, id, "level_branch",       &ts->dts[dts_level_branch]);
            print_descend(obs, id, "while_branch",       &ts->dts[dts_while_branch]);
            print_descend(obs, id, "readdir",            &ts->dts[dts_readdir_call]);
            print_descend(obs, id, "readdir_branch",     &ts->dts[dts_readdir_branch]);
            print_descend(obs, id, "strncmp",            &ts->dts[dts_strncmp_call]);
            print_descend(obs, id, "strncmp_branch",     &ts->dts[dts_strncmp_branch]);
            print_descend(obs, id, "snprintf",           &ts->dts[dts_snprintf_call]);
            print_descend(obs, id, "isdir",              &ts->dts[dts_isdir_cmp]);
            print_descend(obs, id, "isdir_branch",       &ts->dts[dts_isdir_branch]);
            print_descend(obs, id, "access",             &ts->dts[dts_access_call]);
            print_descend(obs, id, "set",                &ts->dts[dts_set]);
            print_descend(obs, id, "clone",              &ts->dts[dts_make_clone]);
            print_descend(obs, id, "pushdir",            &ts->dts[dts_pushdir]);
            print_timer  (obs, id, "sqlsum",             &ts->tts[tts_sqlsum]);
            print_timer  (obs, id, "sqlent",             &ts->tts[tts_sqlent]);
            print_timer  (obs, id, "xattrdone",          &ts->tts[tts_xattrdone_call]);
            print_timer  (obs, id, "detachdb",           &ts->tts[tts_detachdb_call]);
            print_timer  (obs, id, "utime",              &ts->tts[tts_utime_call]);
        }
        print_timer      (obs, id, "closedir",           &ts->tts[tts_closedir_call]);
        print_timer      (obs, id, "free_work",          &ts->tts[tts_free_work]);
    }
    timestamp_set_end_raw(ts->tts[tts_output_timestamps]);
    print_timer          (obs, id, "output_timestamps", &ts->tts[tts_output_timestamps]);
    timestamp_set_end_raw(ts->tts[tts_output_timestamps]); /* take into account previous print */
}

/* sum all descend timestamps of one type */
static uint64_t nsec_descend(sll_t *sll) {
    uint64_t sum = 0;
    sll_loop(sll, node) {
        struct start_end *se = (struct start_end *) sll_node_data(node);
        sum += nsec(se);
    }

    return sum;
}

void timestamps_sum(total_time_t *tt, timestamps_t *ts) {
    pthread_mutex_lock(&tt->mutex);
    tt->lstat             += nsec(&ts->tts[tts_lstat_call]);
    tt->opendir           += nsec(&ts->tts[tts_opendir_call]);
    tt->lstat_db          += nsec(&ts->tts[tts_lstat_db_call]);
    tt->attachdb          += nsec(&ts->tts[tts_attachdb_call]);
    tt->xattrprep         += nsec(&ts->tts[tts_xattrprep_call]);
    tt->sqltsumcheck      += nsec(&ts->tts[tts_sqltsumcheck]);
    tt->sqltsum           += nsec(&ts->tts[tts_sqltsum]);
    tt->get_rollupscore   += nsec(&ts->tts[tts_get_rollupscore_call]);
    tt->descend           += nsec(&ts->tts[tts_descend_call]);
    tt->check_args        += nsec_descend(&ts->dts[dts_check_args]);
    tt->level             += nsec_descend(&ts->dts[dts_level_cmp]);
    tt->level_branch      += nsec_descend(&ts->dts[dts_level_branch]);
    tt->while_branch      += nsec_descend(&ts->dts[dts_while_branch]);
    tt->readdir           += nsec_descend(&ts->dts[dts_readdir_call]);
    tt->readdir_branch    += nsec_descend(&ts->dts[dts_readdir_branch]);
    tt->strncmp           += nsec_descend(&ts->dts[dts_strncmp_call]);
    tt->strncmp_branch    += nsec_descend(&ts->dts[dts_strncmp_branch]);
    tt->snprintf          += nsec_descend(&ts->dts[dts_snprintf_call]);
    tt->isdir             += nsec_descend(&ts->dts[dts_isdir_cmp]);
    tt->isdir_branch      += nsec_descend(&ts->dts[dts_isdir_branch]);
    tt->access            += nsec_descend(&ts->dts[dts_access_call]);
    tt->set               += nsec_descend(&ts->dts[dts_set]);
    tt->clone             += nsec_descend(&ts->dts[dts_make_clone]);
    tt->pushdir           += nsec_descend(&ts->dts[dts_pushdir]);
    tt->sqlsum            += nsec(&ts->tts[tts_sqlsum]);
    tt->sqlent            += nsec(&ts->tts[tts_sqlent]);
    tt->xattrdone         += nsec(&ts->tts[tts_xattrdone_call]);
    tt->detachdb          += nsec(&ts->tts[tts_detachdb_call]);
    tt->utime             += nsec(&ts->tts[tts_utime_call]);
    tt->closedir          += nsec(&ts->tts[tts_closedir_call]);
    tt->free_work         += nsec(&ts->tts[tts_free_work]);
    tt->output_timestamps += nsec(&ts->tts[tts_output_timestamps]);
    pthread_mutex_unlock(&tt->mutex);
}

void timestamps_destroy(timestamps_t *ts) {
    for(int i = 0; i < dts_max; i++) {
        sll_destroy(&ts->dts[i], free);
    }
}
