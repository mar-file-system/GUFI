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
#include <grp.h>
#include <pwd.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "bf.h"
#include "dbutils.h"
#include "utils.h"

/* query to extract data fom databases - this determines the indexing in the print callback */
static const char query_prefix[] = "SELECT type, size, blocks, blksize, inode, nlink, mode, uid, gid, atime, mtime, ctime, linkname, xattr_names FROM";

/* user/group name */
static const char unknown[] = "UNKNOWN";

/* default format used by GNU stat */
static const char default_format[] = "  File: %N\n"
                              "  Size: %-15s Blocks: %-10b IO Block: %-6o %F\n"
                              "Device: %-4Dh/%-5dd    Inode: %-11i Links: %h\n"
                              "Access: (0%a/%A)  Uid: (%5u/%8U)   Gid: (%5g/%8G)\n"
                              "Context: %C\n"
                              "Access: %x\n"
                              "Modify: %y\n"
                              "Change: %z\n"
                              " Birth: %w\n";

/* terse format used by GNU stat -t/--terse when selinux is available */
static const char terse_format[] = "%n %s %b %f %u %g %D %i %h %t %T %X %Y %Z %W %o %C\n";

struct callback_args {
    size_t found;
    const char * path;
    FILE * out;
    const char * format;
};

int print_callback(void * args, int count, char **data, char **columns) {
    (void) columns;

    if (count != 14) {
        fprintf(stderr, "Returned wrong number of columns: %d\n", count);
        return 1;
    }

    struct callback_args * ca = (struct callback_args *) args;
    ca->found = 1;

    FILE *out = ca->out;
    const char *f = ca->format;

    const mode_t mode = atoi(data[6]);

    time_t atime = atoi(data[9]);
    struct tm atm;
    localtime_r(&atime, &atm);
    char atime_str[MAXPATH];

    time_t mtime = atoi(data[10]);
    struct tm mtm;
    localtime_r(&mtime, &mtm);
    char mtime_str[MAXPATH];

    time_t ctime = atoi(data[11]);
    struct tm ctm;
    localtime_r(&ctime, &ctm);
    char ctime_str[MAXPATH];

    while (*f) {
        if (*f != '%') {
            /* handle escape sequences */
            if (*f == '\\') {
                unsigned char escape = *f;
                switch (*++f) {
                    case 'a':
                        escape = '\x07';
                        break;
                    case 'b':
                        escape = '\x08';
                        break;
                    case 'e':
                        escape = '\x1b';
                        break;
                    case 'f':
                        escape = '\x0c';
                        break;
                    case 'n':
                        escape = '\x0a';
                        break;
                    case 'r':
                        escape = '\x0d';
                        break;
                    case 't':
                        escape = '\x09';
                        break;
                    case 'v':
                        escape = '\x0b';
                        break;
                    case '\\':
                        escape = '\x5c';
                        break;
                    case '\'':
                        escape = '\x27';
                        break;
                    case '"':
                        escape = '\x22';
                        break;
                    case '?':
                        escape = '\x3f';
                        break;
                    case 'x':
                        f++;
                        if (('0' <= *f) && (*f <= '9')) {
                            escape = *f - '0';
                        }
                        else if (('a' <= *f) && (*f <= 'f')) {
                            escape = *f - 'a' + 10;
                        }
                        else if (('A' <= *f) && (*f <= 'F')) {
                            escape = *f - 'A' + 10;
                        }
                        else {
                            fprintf(stderr, "gufi_stat: missing hex digit for \\x\n");
                            f -= 2;
                            break;
                        }

                        f++;
                        if (('0' <= *f) && (*f <= '9')) {
                            escape = (escape << 4) | (*f - '0');
                        }
                        else if (('a' <= *f) && (*f <= 'f')) {
                            escape = (escape << 4) | (*f - 'a' + 10);
                        }
                        else if (('A' <= *f) && (*f <= 'F')) {
                            escape = (escape << 4) | (*f - 'A' + 10);
                        }
                        else {
                            f--;
                        }

                        break;
                    case '0': case '1':
                    case '2': case '3':
                    case '4': case '5':
                    case '6': case '7':
                    case '8': case '9':
                        for(int i = 0; i < 3; i++) {
                            if (!(('0' <= *f) && (*f <= '9'))) {
                                break;
                            }
                            escape = (escape << 3) | (*f - '0');
                            f++;
                        }
                        f--;
                        break;
                    default:
                        fwrite("\\", sizeof(char), 1, out);
                        escape = *f;
                        break;
                }
                fwrite(&escape, sizeof(char), 1, out);
            }
            else {
                fwrite(f, sizeof(char), 1, out);
            }
        }
        else {
            f++;

            /* if the first character starts a number */
            int width = 0;
            if (*f && (((*f == '-') ||
                        (*f == '+') ||
                        (('0' <= *f) && (*f <= '9'))))) {

                int multiplier = 1;
                if (*f == '-') {
                    multiplier = -1;
                    f++;
                }

                /* get width */
                while (*f && ('0' <= *f) && (*f <= '9')) {
                    width = (width * 10) + (*f - '0');
                    f++;
                }

                width *= multiplier;
            }

            char format[MAXPATH] = "%";
            if (width) {
                SNPRINTF(format, sizeof(format), "%%%d", width);
            }

            char line[MAXPATH];
            SNPRINTF(line, sizeof(line), "%ss", format);  /* most columns are strings */
            switch (*f) {
                case 'a': /* access rights in octal */
                    SNPRINTF(line, sizeof(line), "%so", format);
                    fprintf(out, line, mode & 0777);
                    break;
                case 'A': /* access rights in human readable form */
                    {
                        char mode_str[11];
                        modetostr(mode_str, 11, mode);
                        fprintf(out, line, mode_str);
                    }
                    break;
                case 'b': /* number of blocks allocated (see %B) */
                    fprintf(out, line, data[2]);
                    break;
                case 'B': /* the size in bytes of each block reported by %b */
                    fprintf(out, line, data[3]);
                    break;
                case 'C': /* SELinux security context string */
                    fprintf(out, line, data[13] + (strlen(data[13])?16:0)); /* offset by "security.selinux" */
                    break;
                case 'd': /* device number in decimal */
                    fprintf(out, line, " ");
                    break;
                case 'D': /* device number in hex */
                    fprintf(out, line, " ");
                    break;
                case 'f': /* raw mode in hex */
                    SNPRINTF(line, sizeof(line), "%sx", format);
                    fprintf(out, line, mode);
                    break;
                case 'F': /* file type */
                    switch (data[0][0]) {
                        case 'f':
                            fprintf(out, line, "regular file");
                            break;
                        case 'l':
                            fprintf(out, line, "symbolic link");
                            break;
                        case 'd':
                            fprintf(out, line, "directory");
                            break;
                        default:
                            break;
                    }
                    break;
                case 'g': /* group ID of owner */
                    fprintf(out, line, data[8]);
                    break;
                case 'G': /* group name of owner */
                    {
                        struct group * grp = getgrgid(atoi(data[8]));
                        const char * group = grp?grp->gr_name:unknown;
                        fprintf(out, line, group);
                    }
                    break;
                case 'h': /* number of hard links */
                    fprintf(out, line, data[5]);
                    break;
                case 'i': /* inode number */
                    fprintf(out, line, data[4]);
                    break;
                case 'm': /* mount point */
                    fprintf(out, line, " ");
                    break;
                case 'n': /* file name */
                    fprintf(out, line, ca->path);
                    break;
                case 'N': /* quoted file name with dereference if symbolic link */
                    {
                        char name[MAXPATH];
                        switch (data[0][0]) {
                            case 'l':
                                SNPRINTF(name, sizeof(name), "'%s' -> '%s'", ca->path, data[12]);
                                break;
                            case 'f':
                            case 'd':
                            default:
                                SNPRINTF(name, sizeof(name), "'%s'", ca->path);
                                break;
                        }
                        fprintf(out, line, name);
                    }
                    break;
                case 'o': /* optimal I/O transfer size hint */
                    fprintf(out, line, data[3]);
                    break;
                case 's': /* total size, in bytes */
                    fprintf(out, line, data[1]);
                    break;
                case 't': /* major device type in hex, for character/block device special files */
                    SNPRINTF(line, sizeof(line), "%sd", format);
                    fprintf(out, line, 0);
                    break;
                case 'T': /* minor device type in hex, for character/block device special files */
                    SNPRINTF(line, sizeof(line), "%sd", format);
                    fprintf(out, line, 0);
                    break;
                case 'u': /* user ID of owner */
                    fprintf(out, line, data[7]);
                    break;
                case 'U': /* user name of owner */
                    {
                        struct passwd * usr = getpwuid(atoi(data[7]));
                        const char * user = usr?usr->pw_name:unknown;
                        fprintf(out, line, user);
                    }
                    break;
                case 'w': /* time of file birth, human-readable; - if unknown */
                    fprintf(out, line, "-");
                    break;
                case 'W': /* time of file birth, seconds since Epoch; 0 if unknown */
                    fprintf(out, line, "0");
                    break;
                case 'x': /* time of last access, human-readable */
                    strftime(atime_str, MAXPATH, "%F %T %z", &atm);
                    fprintf(out, line, atime_str);
                    break;
                case 'X': /* time of last access, seconds since Epoch */
                    strftime(atime_str, MAXPATH, "%s", &atm);
                    fprintf(out, line, atime_str);
                    break;
                case 'y': /* time of last modification, human-readable */
                    strftime(mtime_str, MAXPATH, "%F %T %z", &mtm);
                    fprintf(out, line, mtime_str);
                    break;
                case 'Y': /* time of last modification, seconds since Epoch */
                    strftime(mtime_str, MAXPATH, "%s", &mtm);
                    fprintf(out, line, mtime_str);
                    break;
                case 'z': /* time of last change, human-readable */
                    strftime(ctime_str, MAXPATH, "%F %T %z", &ctm);
                    fprintf(out, line, ctime_str);
                    break;
                case 'Z': /* time of last change, seconds since Epoch */
                    strftime(ctime_str, MAXPATH, "%s", &ctm);
                    fprintf(out, line, ctime_str);
                    break;
                default:
                    fwrite("?", sizeof(char), 1, out);
                    break;
            }
        }

        f++;
    }

    fflush(out);

    return 0;
}

int process_path(const char *path, FILE *out, const char *format) {
    char dbname[MAXPATH];
    char table[MAXSQL];
    char where[MAXSQL];
    char query[MAXSQL];
    struct stat st;

    /* path is directory */
    if ((lstat(path, &st) == 0) && S_ISDIR(st.st_mode)) {
        SNPRINTF(dbname, sizeof(dbname), "%s/" DBNAME, path);
        SNPRINTF(table, sizeof(table), "summary");
        SNPRINTF(where, sizeof(where), "WHERE isroot == 1");
    }
    /*
     * path does exist:    path is in the filesystem the index is on,
     *                     rather than in the index (e.g. db.db)
     *
     * path doesn't exist: path is a file/link name that might exist
     *                     within the index
     *
     * either way, search index at dirname(path)
     */
    else {
        /* remove basename from the path */
        char parent[MAXPATH];
        char name[MAXPATH];
        shortpath(path, parent, name);

        SNPRINTF(dbname, sizeof(dbname), "%s/" DBNAME, parent);
        SNPRINTF(table, sizeof(table), "entries");
        SNPRINTF(where, sizeof(where), "WHERE name == '%s'", name);
    }

    SNPRINTF(query, sizeof(query), "%s %s %s;", query_prefix, table, where);

    struct callback_args ca;
    ca.found = 0;
    ca.path = path;
    ca.out = out;
    ca.format = format;

    int rc = 0;
    sqlite3 *db = NULL;
    if ((db = opendb(dbname, SQLITE_OPEN_READONLY, 0, 1
                     , NULL, NULL
                     #if defined(DEBUG) && defined(PER_THREAD_STATS)
                     , NULL, NULL
                     , NULL, NULL
                     #endif
                     ))) {
        /* query the database */
        char *err = NULL;
        if (sqlite3_exec(db, query, print_callback, &ca, &err) != SQLITE_OK) {
            fprintf(stderr, "gufi_stat: failed to query database in '%s': %s\n", path, err);
            rc = 1;
        }

        sqlite3_free(err);

        /* if the query was successful, but nothing was found, error */
        if (!ca.found) {
            fprintf(stderr, "gufi_stat: cannot stat '%s': No such file or directory\n", path);
            rc = 1;
        }
    }
    else {
        fprintf(stderr, "gufi_stat: cannot stat '%s': No such file or directory\n", path);
        rc = 1;
    }

    /* close no matter what to avoid memory leaks */
    closedb(db);

    return rc;
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
    struct input in;
    int idx = parse_cmd_line(argc, argv, "hHf:j", 1, "path ...", &in);
    if (in.helped)
        sub_help();
    if (idx < 0)
        return -1;

    const char *format = default_format;

    /* the print format has precedence over the terse format */
    if (in.format_set) {
        format = in.format;
    }
    else if (in.terse) {
        format = terse_format;
    }

    /* process all input paths */
    int rc = 0;
    for(int i = idx; i < argc; i++) {
        rc |= process_path(argv[i], stdout, format);
    }

    return rc;
}
