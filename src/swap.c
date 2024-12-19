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
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "swap.h"
#include "utils.h"

/*
 * Parent directory of prefix should already exist. It will not be created.
 * If the prefix is to a directory, include the trailing slash.
 *
 * Filenames don't matter because the files are deleted immediately.
 * Even if the files existed before, they are truncated, overwritten, and deleted immediately.
 */
static int swap_open(const char *prefix, const size_t id) {
    /* Not checking arguments */

    char name[MAXPATH];
    SNPRINTF(name, sizeof(name), "%s%jd.swap.%" PRIu64, prefix, (intmax_t) getpid(), id);

    /* not using O_TMPFILE to avoid needing _GNU_SOURCE */
    const int fd = open(name, O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open %s: %s (%d)\n",
                name, strerror(err), err);
        return -1;
    }

    /* O_TRUNC requires that the path is a regular file, so no need to check type */

    /* remove from filesystem immediately */
    if (remove(name) != 0) {
        const int err = errno;
        fprintf(stderr, "Warning: Could not remove swap file %s from filesystem: %s (%d)\n",
                name, strerror(err), err);
    }

    return fd;
}

void swap_init(struct Swap *swap) {
    /* Not checking argument */

    memset(swap, 0, sizeof(*swap));
    swap->write.fd = -1;
    swap->read.fd = -1;
}

int swap_start(struct Swap *swap, const char *prefix, const uint64_t id) {
    /* Not checking other arguments */

    if (!prefix) {
        return 1;
    }

    swap->prefix = prefix;
    swap->id = id;
    swap->write.fd = swap_open(swap->prefix, swap->id);
    swap->write.count = 0;
    swap->write.size = 0;

    if (swap->write.fd < 0) {
        swap_destroy(swap);
        return 1;
    }

    return 0;
}

int swap_restart(struct Swap *swap, const char *prefix, const uint64_t id) {
    /* Not checking arguments */

    if (swap->write.fd > -1) {
        close(swap->write.fd);
        /* no need to set to -1 */
    }

    /* not resetting counters */
    return swap_start(swap, prefix, id);
}

int swap_read_prep(struct Swap *swap) {
    /* Not checking argument */

    /*
     * not closing read.fd if it is > -1 so that if read_done is not
     * called before calling this function again will cause a file
     * descriptor to be left open
     */

    swap->read.fd = -1;
    swap->read.count = 0;
    swap->read.size = 0;

    /* no need to move file descriptor */
    if (swap->write.count == 0) {
        return 1;
    }

    swap->read = swap->write;
    swap->write.fd = swap_open(swap->prefix, swap->id);
    swap->write.count = 0;
    swap->write.size = 0;

    if (lseek(swap->read.fd, 0, SEEK_SET) == (off_t) -1) {
        const int err = errno;
        fprintf(stderr, "Error: Could not reset swap file (%d): %s (%d)\n",
                swap->read.fd, strerror(err), err);
        return 1;
    }

    if (swap->write.fd < 0) {
        const int err = errno;
        fprintf(stderr, "Error: Could not open new swap file: %s (%d)\n",
                strerror(err), err);
        return 1;
    }

    return 0;
}

void swap_read_done(struct Swap *swap) {
    if (swap->read.fd > -1) {
        close(swap->read.fd);
    }

    swap->read.fd = -1;
    swap->read.count = 0;
    swap->read.size = 0;
}

void swap_stop(struct Swap *swap) {
    /* Not checking argument */

    if (swap->read.fd > -1) {
        close(swap->read.fd);
        swap->read.fd = -1;
    }

    if (swap->write.fd > -1) {
        close(swap->write.fd);
        swap->write.fd = -1;
    }
}

void swap_destroy(struct Swap *swap) {
    /* Not checking argument */
    swap_init(swap);
}
