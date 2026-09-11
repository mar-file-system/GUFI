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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "popen_argv.h"

struct popen_argv_ret {
    pid_t pid;
    int fds[2];
};

popen_argv_t *popen_argv(const char **argv, const int redirect_stdin) {
    if (!argv) {
        return NULL;
    }

    int to_parent[2];              /* for parent to read */
    if (pipe(to_parent) != 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not create read pipe: %s (%d)\n",
                strerror(err), err);
        return NULL;
    }

    int to_child[2] = { -1, -1 };  /* for parent to write */
    if (redirect_stdin) {
        if (pipe(to_child) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Could not create write pipe: %s (%d)\n",
                    strerror(err), err);
            close(to_parent[1]);
            close(to_parent[0]);
            return NULL;
        }
    }

    const pid_t pid = fork();
    if (pid == -1) {
        const int err = errno;
        fprintf(stderr, "Error: Could not fork: %s (%d)\n",
                strerror(err), err);
        close(to_parent[1]);
        close(to_parent[0]);
        close(to_child[1]);
        close(to_child[0]);
        return NULL;
    }

    if (pid == 0) {     /* child */
        close(to_parent[0]);  /* child does not read parent's output */
        close(to_child[1]);   /* child does not write to parent's input */

        if (redirect_stdin) {
            /* replace stdin */
            if (dup2(to_child[0], STDIN_FILENO) != STDIN_FILENO) {
                const int err = errno;
                fprintf(stderr, "Error: Failed to replace stdin: %s (%d)\n",
                        strerror(err), err);
                close(to_parent[1]);
                close(to_child[0]);
                exit(1);
            }
        }

        /* replace stdout */
        if (dup2(to_parent[1], STDOUT_FILENO) != STDOUT_FILENO) {
            const int err = errno;
            fprintf(stderr, "Error: Failed to replace stdout: %s (%d)\n",
                    strerror(err), err);
            close(to_parent[1]);
            close(to_child[0]);
            exit(1);
        }

        /* do not need original pipe end points */
        close(to_parent[1]);
        close(to_child[0]);

        /* run the command */
        if (execvp(argv[0], (char **) argv) != 0) {
            const int err = errno;
            fprintf(stderr, "Error: Failed to exec command: %s (%d)\n",
                    strerror(err), err);
        }

        /* reaching here for any reason is an error */
        exit(1);
    }
    else {
        close(to_parent[1]);
        close(to_child[0]);
    }

    /* parent */
    popen_argv_t *ret = calloc(1, sizeof(*ret));
    ret->pid = pid;
    ret->fds[0] = to_child[1];  /* parent's input is child's output */
    ret->fds[1] = to_parent[0]; /* parent's output is child's input */
    return ret;
}

int popen_argv_in(popen_argv_t *ret) {
    return ret?ret->fds[0]:-1;
}

int popen_argv_out(popen_argv_t *ret) {
    return ret?ret->fds[1]:-1;
}

int popen_argv_close(popen_argv_t *ret) {
    if (!ret) {
        return 0;
    }

    int rc = -1;

    close(ret->fds[0]);

    int status = 0;
    if (waitpid(ret->pid, &status, 0) != 0) {
        if (WIFEXITED(status)) {
            rc = WEXITSTATUS(status);
        }
    }

    close(ret->fds[1]);
    free(ret);

    return rc;
}
