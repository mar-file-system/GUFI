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



#include <grp.h>
#include <math.h>
#include <pwd.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "addqueryfuncs.h"
#include "utils.h"

static void uidtouser(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *text = (char *) sqlite3_value_text(argv[0]);

    const int fuid = atoi(text);
    struct passwd *fmypasswd = getpwuid(fuid);
    const char *show = fmypasswd?fmypasswd->pw_name:text;

    sqlite3_result_text(context, show, -1, SQLITE_TRANSIENT);
}

static void gidtogroup(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *text = (char *) sqlite3_value_text(argv[0]);

    const int fgid = atoi(text);
    struct group *fmygroup = getgrgid(fgid);
    const char *show = fmygroup?fmygroup->gr_name:text;

    sqlite3_result_text(context, show, -1, SQLITE_TRANSIENT);
}

static void modetotxt(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;
    int fmode;
    char tmode[64];
    fmode = sqlite3_value_int(argv[0]);
    modetostr(tmode, sizeof(tmode), fmode);
    sqlite3_result_text(context, tmode, -1, SQLITE_TRANSIENT);
}

static void sqlite3_strftime(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    const char *fmt = (char *) sqlite3_value_text(argv[0]); /* format    */
    const time_t t = sqlite3_value_int64(argv[1]);          /* timestamp */

    char buf[MAXPATH];
    #ifdef LOCALTIME_R
    struct tm tm;
    strftime(buf, sizeof(buf), fmt, localtime_r(&t, &tm));
    #else
    strftime(buf, sizeof(buf), fmt, localtime(&t));
    #endif
    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);
}

/* uint64_t goes up to E */
static const char SIZE[] = {'K', 'M', 'G', 'T', 'P', 'E'};

/*
 * Returns the number of blocks required to store a given size
 * Unfilled blocks count as one full block (round up)
 *
 * This function attempts to replicate ls output and is mainly
 * intended for gufi_ls, so use with caution.
 *
 * blocksize(1024, "K")    -> 1K
 * blocksize(1024, "1K")   -> 1
 * blocksize(1024, "KB")   -> 2KB
 * blocksize(1024, "1KB")  -> 2
 * blocksize(1024, "KiB")  -> 1K
 * blocksize(1024, "1KiB") -> 1
 */
static void blocksize(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *size_s = (const char *) sqlite3_value_text(argv[0]);
    const char *unit_s = (const char *) sqlite3_value_text(argv[1]);
    const size_t unit_s_len = strlen(unit_s);

    uint64_t size = 0;
    if (sscanf(size_s, "%" PRIu64, &size) != 1) {
        sqlite3_result_error(context, "Bad blocksize size", -1);
        return;
    }

    /* whether or not a coefficent was found - affects printing */
    uint64_t unit_size = 0;
    const int coefficient_found = sscanf(unit_s, "%" PRIu64, &unit_size);
    if (coefficient_found == 1) {
        if (unit_size == 0) {
            sqlite3_result_error(context, "Bad blocksize unit", -1);
            return;
        }
    }
    else {
        /* if there were no numbers, default to 1 */
        unit_size = 1;
    }

    /*
     * get block size suffix i.e. 1KB -> KB
     */
    const char *unit = unit_s;
    {
        /*
         * find first non-numerical character
         * decimal points are not accepted, and will break this loop
         */
        size_t offset = 0;
        while ((offset < unit_s_len) &&
               (('0' <= unit[offset]) && (unit[offset] <= '9'))) {
            offset++;
        }

        unit += offset;
    }

    const size_t len = strlen(unit);

    /* suffix is too long */
    if (len > 3) {
        sqlite3_result_error(context, "Bad blocksize unit", -1);
        return;
    }

    /* suffix is optional */
    if (len) {
        if ((len > 1) && (unit[len - 1] != 'B')) {
            sqlite3_result_error(context, "Bad blocksize unit", -1);
            return;
        }

        uint64_t multiplier = 1024;
        if (len == 2) {
            multiplier = 1000;
        }
        else if (len == 3) {
            if (unit[1] != 'i') {
                sqlite3_result_error(context, "Bad blocksize unit", -1);
                return;
            }
        }

        int found = 0;
        for(size_t i = 0; i < sizeof(SIZE); i++) {
            unit_size *= multiplier;
            if (unit[0] == SIZE[i]) {
                found = 1;
                break;
            }
        }

        if (!found) {
            sqlite3_result_error(context, "Bad blocksize unit", -1);
            return;
        }
    }

    const uint64_t blocks = (size / unit_size) + (!!(size % unit_size));

    char buf[MAXPATH];
    size_t buf_len = snprintf(buf, sizeof(buf), "%" PRIu64, blocks);

    /* add unit to block count */
    if (!coefficient_found) {
        buf_len += snprintf(buf + buf_len, sizeof(buf) - buf_len, "%s", unit);
    }

    sqlite3_result_text(context, buf, buf_len, SQLITE_TRANSIENT);
}

/* Returns a string containg the size with as large of a unit as reasonable */
static void human_readable_size(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    char buf[MAXPATH];

    const char *size_s = (const char *) sqlite3_value_text(argv[0]);
    double size = 0;

    if (sscanf(size_s, "%lf", &size) != 1) {
        sqlite3_result_error(context, "Bad size", -1);
        return;
    }

    size_t unit_index = 0;
    while (size >= 1024) {
        size /= 1024;
        unit_index++;
    }

    if (unit_index == 0) {
        snprintf(buf, sizeof(buf), "%.1f", size);
    }
    else {
        snprintf(buf, sizeof(buf), "%.1f%c", size, SIZE[unit_index - 1]);
    }

    sqlite3_result_text(context, buf, -1, SQLITE_TRANSIENT);
}

static void sqlite_basename(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    char *path = (char *) sqlite3_value_text(argv[0]);

    if (!path) {
        sqlite3_result_text(context, "", 0, SQLITE_TRANSIENT);
        return;
    }

    const size_t path_len = strlen(path);

    /* remove trailing slashes */
    const size_t trimmed_len = trailing_non_match_index(path, path_len, "/", 1);
    if (!trimmed_len) {
        sqlite3_result_text(context, "/", 1, SQLITE_STATIC);
        return;
    }

    /* basename(work->name) will be the same as the first part of the input path, so remove it */
    const size_t offset = trailing_match_index(path, trimmed_len, "/", 1);

    const size_t bn_len = trimmed_len - offset;
    char *bn = path + offset;

    sqlite3_result_text(context, bn, bn_len, SQLITE_STATIC);
}

/* prefix should end with an opening single quote */
static void return_error(sqlite3_context *context,
                         const char *prefix, const size_t prefix_size,
                         const char *str) {
    const size_t str_len = strlen(str);
    const size_t err_len = prefix_size - 1 + str_len + 1; /* closing quote */
    char *err = malloc(err_len + 1);
    SNFORMAT_S(err, err_len + 1, 5,
               prefix, prefix_size - 1,
               " ", (size_t) 1,
               "'", (size_t) 1,
               str, str_len,
               "'", (size_t) 1);
    sqlite3_result_error(context, err, err_len);
    free(err);
}

/*
 * run a command and get one line from stdout
 *
 * caller should free line
 */
static int lineop(sqlite3_context *context, const char *cmd, char **line, size_t *line_len) {
    FILE *p = popen(cmd, "r");
    if (p == NULL) {
        static const char ERR_PREFIX[] = "lineop: popen failed to run";
        return_error(context, ERR_PREFIX, sizeof(ERR_PREFIX), cmd);
        return -1;
    }

    size_t alloc = 0;
    ssize_t len = getline(line, &alloc, p);
    pclose(p);

    if (len < 0) {
        static const char ERR_PREFIX[] = "lineop: failed to read result of";
        return_error(context, ERR_PREFIX, sizeof(ERR_PREFIX), cmd);
        free(*line);
        return -1;
    }

    /* only newline, so no value */
    if (len == 1) {
        static const char ERR_PREFIX[] = "lineop: did not get result from";
        return_error(context, ERR_PREFIX, sizeof(ERR_PREFIX), cmd);
        free(*line);
        return -1;
    }

    /* remove newline */
    (*line)[--len] = '\0';
    *line_len = len;

    return 0;
}

/* run a command and get one line from stdout */
static void strop(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *cmd = (const char *) sqlite3_value_text(argv[0]);
    char *line = NULL;
    size_t len = 0;

    if (lineop(context, cmd, &line, &len) != 0) {
        return;
    }

    sqlite3_result_text(context, line, len, SQLITE_TRANSIENT);
    free(line);
}

/* run a command and get one int from stdout */
static void intop(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *cmd = (const char *) sqlite3_value_text(argv[0]);
    char *line = NULL;
    size_t len = 0;

    if (lineop(context, cmd, &line, &len) != 0) {
        return;
    }

    int retval = 0;
    if (sscanf(line, "%d", &retval) != 1) {
        static const char ERR_PREFIX[] = "intop: could not parse result from";
        return_error(context, ERR_PREFIX, sizeof(ERR_PREFIX), line);
        free(line);
        return;
    }

    free(line);

    sqlite3_result_int64(context, retval);
}

/* run a command and get all data from stdout */
static void blobop(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    const char *cmd = (const char *) sqlite3_value_text(argv[0]);
    FILE *p = popen(cmd, "r");
    if (p == NULL) {
        static const char ERR_PREFIX[] = "blobop: popen failed to run";
        return_error(context, ERR_PREFIX, sizeof(ERR_PREFIX), cmd);
        return;
    }

    size_t alloc = 256; /* magic number */
    char *data = malloc(alloc);
    size_t len = 0;
    size_t got = 0;

    while ((got = fread(data + len, sizeof(char), alloc - len, p)) == (alloc - len)) {
        len += got;

        if (len == alloc) {
            alloc *= 2;
            char *new_buf = realloc(data, alloc);
            if (!new_buf) {
                sqlite3_result_error_nomem(context);
                free(data);
                pclose(p);
                return;
            }

            data = new_buf;
        }
    }

    pclose(p);

    if ((char) got == EOF) {
        sqlite3_result_error_code(context, SQLITE_ERROR);
        free(data);
        return;
    }

    len += got;

    sqlite3_result_blob(context, data, len, SQLITE_TRANSIENT);
    free(data);
}

/*
 * One pass standard deviation (sample)
 * https://mathcentral.uregina.ca/QQ/database/QQ.09.06/h/murtaza1.html
 */
typedef struct {
    double sum;
    double sum_sq;
    uint64_t count;
} stdev_t;

static void stdev_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    stdev_t *data = (stdev_t *) sqlite3_aggregate_context(context, sizeof(*data));
    const double value = sqlite3_value_double(argv[0]);

    data->sum += value;
    data->sum_sq += value * value;
    data->count++;
}

static void stdevs_final(sqlite3_context *context) {
    stdev_t *data = (stdev_t *) sqlite3_aggregate_context(context, sizeof(*data));

    if (data->count < 2) {
        sqlite3_result_null(context);
    }
    else {
        const double variance = ((data->count * data->sum_sq) - (data->sum * data->sum)) / (data->count * (data->count - 1));
        sqlite3_result_double(context, sqrt(variance));
    }
}

static void stdevp_final(sqlite3_context *context) {
    stdev_t *data = (stdev_t *) sqlite3_aggregate_context(context, sizeof(*data));

    if (data->count < 2) {
        sqlite3_result_null(context);
    }
    else {
        const double variance = ((data->count * data->sum_sq) - (data->sum * data->sum)) / (data->count * data->count);
        sqlite3_result_double(context, sqrt(variance));
    }
}

static void median_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;
    sll_t *data = (sll_t *) sqlite3_aggregate_context(context, sizeof(*data));
    if (sll_get_size(data) == 0) {
        sll_init(data);
    }

    const double value = sqlite3_value_double(argv[0]);
    sll_push(data, (void *) (uintptr_t) value);
}

static int cmp_double(const void *lhs, const void *rhs) {
    return * (double *) lhs - * (double *) rhs;
}

static void median_final(sqlite3_context *context) {
    sll_t *data = (sll_t *) sqlite3_aggregate_context(context, sizeof(*data));

    const uint64_t count = sll_get_size(data);
    double median = 0;

    /* skip some mallocs */
    if (count == 0) {
        sqlite3_result_null(context);
        goto cleanup;
    }
    else if (count == 1) {
        median = (double) (uintptr_t) sll_node_data(sll_head_node(data));
        goto ret_median;
    }
    else if (count == 2) {
        median = ((double) (uintptr_t) sll_node_data(sll_head_node(data)) +
                  (double) (uintptr_t) sll_node_data(sll_tail_node(data))) / 2.0;
        goto ret_median;
    }

    const uint64_t half = count / 2;

    double *arr = malloc(count * sizeof(double));
    size_t i = 0;
    sll_loop(data, node) {
        arr[i++] = (double) (uintptr_t) sll_node_data(node);
    }

    qsort(arr, count, sizeof(double), cmp_double);

    median = arr[half];
    if (!(count & 1)) {
        median += arr[half - 1];
        median /= 2.0;
    }
    free(arr);

    /* median = quickselect(data, count, half); */
    /* if (!(count & 1)) { */
    /*     median += quickselect(data, count, half - 1); */
    /*     median /= 2.0; */
    /* } */

  ret_median:
    sqlite3_result_double(context, median);

  cleanup:
    sll_destroy(data, NULL);
}

/* return the directory you are currently in */
static void path(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);
    size_t user_dirname_len = work->orig_root.len + work->name_len - work->root_parent.len - work->root_basename_len;
    char *user_dirname = malloc(user_dirname_len + 1);

    SNFORMAT_S(user_dirname, user_dirname_len + 1, 2,
               work->orig_root.data, work->orig_root.len,
               work->name + work->root_parent.len + work->root_basename_len, work->name_len - work->root_parent.len - work->root_basename_len);

    sqlite3_result_text(context, user_dirname, user_dirname_len, free);
}

/* return the basename of the directory you are currently in */
static void epath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);

    sqlite3_result_text(context, work->name + work->name_len - work->basename_len,
                        work->basename_len, SQLITE_STATIC);
}

/* return the fullpath of the directory you are currently in */
static void fpath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc; (void) argv;
    struct work *work = (struct work *) sqlite3_user_data(context);

    if (!work->fullpath) {
        work->fullpath = realpath(work->name, NULL);
        work->fullpath_len = strlen(work->fullpath);
    }

    sqlite3_result_text(context, work->fullpath, work->fullpath_len, SQLITE_STATIC);
}

/*
 * Usage:
 *     SELECT rpath(sname, sroll)
 *     FROM vrsummary;
 *
 *     SELECT rpath(sname, sroll) || "/" || name
 *     FROM vrpentries;
 */
static void rpath(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    (void) argc;

    /* work->name contains the current directory being operated on */
    struct work *work = (struct work *) sqlite3_user_data(context);
    const int rollupscore = sqlite3_value_int(argv[1]);

    size_t user_dirname_len = 0;
    char *user_dirname = NULL;

    const size_t root_len = work->root_parent.len + work->root_basename_len;

    if (rollupscore == 0) { /* use work->name */
        user_dirname_len = work->orig_root.len + work->name_len - root_len;
        user_dirname = malloc(user_dirname_len + 1);

        SNFORMAT_S(user_dirname, user_dirname_len + 1, 2,
                   work->orig_root.data, work->orig_root.len,
                   work->name + root_len, work->name_len - root_len);
    }
    else { /* reconstruct full path out of argv[0] */
        refstr_t input;
        input.data = (char *) sqlite3_value_text(argv[0]);
        input.len  = strlen(input.data);

        /*
         * fullpath = work->name[:-work->basename_len] + input
         */
        const size_t fullpath_len = work->name_len - work->basename_len + input.len;
        char *fullpath = malloc(fullpath_len + 1);
        SNFORMAT_S(fullpath, fullpath_len + 1, 2,
                   work->name, work->name_len - work->basename_len,
                   input.data, input.len);

        /*
         * replace fullpath prefix with original user input
         */
        user_dirname_len = work->orig_root.len + fullpath_len - root_len;
        user_dirname = malloc(user_dirname_len + 1);
        SNFORMAT_S(user_dirname, user_dirname_len + 1, 2,
                   work->orig_root.data, work->orig_root.len,
                   fullpath + root_len, fullpath_len - root_len);

        free(fullpath);
    }

    sqlite3_result_text(context, user_dirname, user_dirname_len, free);
}

static void relative_level(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc; (void) argv;

    size_t level = (size_t) (uintptr_t) sqlite3_user_data(context);
    sqlite3_result_int64(context, level);
}

static void starting_point(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc; (void) argv;

    refstr_t *root = (refstr_t *) sqlite3_user_data(context);
    sqlite3_result_text(context, root->data, root->len, SQLITE_STATIC);
}

int addqueryfuncs(sqlite3 *db) {
    return !(
        (sqlite3_create_function(db,   "uidtouser",           1,   SQLITE_UTF8,
                                 NULL, &uidtouser,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "gidtogroup",          1,   SQLITE_UTF8,
                                 NULL, &gidtogroup,                NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "modetotxt",           1,   SQLITE_UTF8,
                                 NULL, &modetotxt,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "strftime",            2,   SQLITE_UTF8,
                                 NULL, &sqlite3_strftime,          NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "blocksize",           2,   SQLITE_UTF8,
                                 NULL, &blocksize,                 NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "human_readable_size", 1,   SQLITE_UTF8,
                                 NULL, &human_readable_size,       NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "basename",            1,   SQLITE_UTF8,
                                 NULL, &sqlite_basename,           NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "strop",               1,   SQLITE_UTF8,
                                 NULL, &strop,                     NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "intop",               1,   SQLITE_UTF8,
                                 NULL, &intop,                     NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "blobop",              1,   SQLITE_UTF8,
                                 NULL, &blobop,                    NULL, NULL)   == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevs",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevs_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "stdevp",              1,   SQLITE_UTF8,
                                 NULL, NULL,  stdev_step,          stdevp_final) == SQLITE_OK) &&
        (sqlite3_create_function(db,   "median",              1,   SQLITE_UTF8,
                                 NULL, NULL,  median_step,         median_final) == SQLITE_OK) &&
        addhistfuncs(db)
        );
}

int addqueryfuncs_with_context(sqlite3 *db, struct work *work) {
    return !(
        (sqlite3_create_function(db,  "path",                      0, SQLITE_UTF8,
                                 work,                             &path,           NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "epath",                     0, SQLITE_UTF8,
                                 work,                             &epath,          NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "fpath",                     0, SQLITE_UTF8,
                                 work,                             &fpath,          NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "rpath",                     2, SQLITE_UTF8,
                                 work,                             &rpath,          NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "starting_point",            0,  SQLITE_UTF8,
                                 (void *) &work->orig_root,        &starting_point, NULL, NULL) == SQLITE_OK) &&
        (sqlite3_create_function(db,  "level",                     0,  SQLITE_UTF8,
                                 (void *) (uintptr_t) work->level, &relative_level, NULL, NULL) == SQLITE_OK)
        );
}
