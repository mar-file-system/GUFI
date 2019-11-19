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



#include <dirent.h>
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "bf.h"
#include "dbutils.h"
#include "utils.h"

extern int errno;

const char query_prefix[] = "SELECT type, size, blocks, blksize, inode, nlink, mode, uid, gid, atime, mtime, ctime, linkname FROM %s %s";
const char unknown[] = "UNKNOWN"; /* user/group name */

struct callback_args {
    size_t found;
    const char * path;
};

int print_callback(void * args, int count, char **data, char **columns) {
    if (count != 13) {
        fprintf(stderr, "Returned wrong number of columns: %d\n", count);
        return 1;
    }

    struct callback_args * ca = (struct callback_args *) args;
    ca->found = 1;

    /* process row */
    char name[MAXPATH + MAXPATH + MAXPATH];
    char type[MAXPATH];
    switch (data[0][0]) {
        case 'f':
            snprintf(name, MAXPATH, "'%s'", ca->path);
            snprintf(type, MAXPATH, "regular file");
            break;
        case 'l':
            snprintf(name, MAXPATH, "'%s' -> '%s'", ca->path, data[12]);
            snprintf(type, MAXPATH, "symbolic link");
            break;
        case 'd':
            snprintf(name, MAXPATH, "'%s'", ca->path);
            snprintf(type, MAXPATH, "directory");
            break;
        default:
            type[0] = '\0';
            break;
    }

    char mode[11];
    modetostr(mode, atoi(data[6]));

    struct passwd * usr = getpwuid(atoi(data[7]));
    const char * user = usr?usr->pw_name:unknown;

    struct group * grp = getgrgid(atoi(data[8]));
    const char * group = grp?grp->gr_name:unknown;

    time_t atime = atoi(data[9]);
    struct tm * atm = localtime(&atime);
    char atime_str[MAXPATH];
    strftime(atime_str, MAXPATH, "%F %T %z", atm);

    time_t mtime = atoi(data[10]);
    struct tm * mtm = localtime(&mtime);
    char mtime_str[MAXPATH];
    strftime(mtime_str, MAXPATH, "%F %T %z", mtm);

    time_t ctime = atoi(data[11]);
    struct tm * ctm = localtime(&ctime);
    char ctime_str[MAXPATH];
    strftime(ctime_str, MAXPATH, "%F %T %z", ctm);

    /* print */
    fprintf(stdout, "  File: %s\n", name);
    fprintf(stdout, "  Size: %-15s Blocks: %-10s IO Block: %-6s %s\n", data[1], data[2], data[3], type);
    fprintf(stdout, "Device: %-5s/%-9s Inode: %-11s Links: %s\n", "", "", data[4], data[5]);
    fprintf(stdout, "Access: (0%3o/%10s)  Uid: (%5s/%8s)   Gid: (%5s/%8s)\n", atoi(data[6]) & 0777, mode, data[7], user, data[8], group);
    fprintf(stdout, "Access: %s\n", atime_str);
    fprintf(stdout, "Modify: %s\n", mtime_str);
    fprintf(stdout, "Change: %s\n", ctime_str);
    fprintf(stdout, " Birth: -\n");

    return 0;
}

int process_path(const char * path) {
    char dbname[MAXPATH + MAXPATH];
    sqlite3 * db = NULL;
    char query[MAXSQL + MAXSQL + MAXSQL];

    /* assume the provided path is a directory */
    DIR * dir = opendir(path);
    const int err = errno;
    if (!dir) {
        /* searched for it in the entries table */

        /* remove file name from the path */
        char dir[MAXPATH];
        char name[MAXPATH];
        shortpath(path, dir, name);

        snprintf(dbname, sizeof(dbname), "%s/" DBNAME, dir);

        char where[MAXSQL + MAXSQL + MAXSQL / 2];
        snprintf(where, sizeof(where), "WHERE name == '%s'", name);
        snprintf(query, sizeof(query), query_prefix, "entries", where);
    }
    else {
        snprintf(dbname, sizeof(dbname), "%s/" DBNAME, path);
        snprintf(query, sizeof(query), query_prefix, "summary", "");
    }

    closedir(dir);

    struct callback_args ca;
    ca.found = 0;
    ca.path = path;

    if ((db = opendb(dbname, RDONLY, 0, 1,
                     NULL, NULL
                     #ifdef DEBUG
                     , NULL, NULL
                     , NULL, NULL
                     , NULL, NULL
                     , NULL, NULL
                     #endif
                     ))) {
        /* query the database */
        char *err = NULL;
        if (sqlite3_exec(db, query, print_callback, &ca, &err) != SQLITE_OK) {
            fprintf(stderr, "Error: %s: %s\n", err, dbname);
            sqlite3_free(err);
            ca.found = 0;
        }
    }

    /* close no matter what to avoid memory leaks */
    closedb(db);

    if (!ca.found) {
        fprintf(stderr, "gufi_stat: cannot stat '%s': %s\n", path, strerror(err));
        return 1;
    }

    return 0;
}

void sub_help() {
    printf("path                 path to stat\n");
    printf("\n");
}

int main(int argc, char *argv[])
{
    /* process input args - all programs share the common 'struct input', */
    /* but allow different fields to be filled at the command-line. */
    /* Callers provide the options-string for get_opt(), which will */
    /* control which options are parsed for each program. */
    int idx = parse_cmd_line(argc, argv, "hH", 1, "path ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    /* process all input paths */
    for(int i = idx; i < argc; i++) {
        process_path(argv[i]);
    }

    return 0;
}
