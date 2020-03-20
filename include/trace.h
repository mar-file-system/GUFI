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



#ifndef GUFI_TRACE_H
#define GUFI_TRACE_H

#include <stdio.h>

#include "bf.h"

#define INT_CHARS 20 /* maximum number of characters of   */
                     /* a 64 bit integer is 20 characters */

#define STANZA_SIZE MAXPATH   + 1 + /* name */       \
                    1         + 1 + /* type */       \
                    INT_CHARS + 1 + /* st_ino */     \
                    INT_CHARS + 1 + /* st_mode */    \
                    INT_CHARS + 1 + /* st_nlink */   \
                    INT_CHARS + 1 + /* st_uid */     \
                    INT_CHARS + 1 + /* st_gid */     \
                    INT_CHARS + 1 + /* st_size */    \
                    INT_CHARS + 1 + /* st_blksize */ \
                    INT_CHARS + 1 + /* st_blocks */  \
                    INT_CHARS + 1 + /* st_atime */   \
                    INT_CHARS + 1 + /* st_mtime */   \
                    INT_CHARS + 1 + /* st_ctime */   \
                    MAXPATH   + 1 + /* linkname */   \
                    MAXXATTR  + 1 + /* xattr */      \
                    INT_CHARS + 1 + /* crtime */     \
                    INT_CHARS + 1 + /* ossint1 */    \
                    INT_CHARS + 1 + /* ossint2 */    \
                    INT_CHARS + 1 + /* ossint3 */    \
                    INT_CHARS + 1 + /* ossint4 */    \
                    MAXXATTR  + 1 + /* osstext1 */   \
                    MAXXATTR  + 1 + /* osstext2 */   \
                    INT_CHARS + 1 + /* pinode */     \
                    1 +             /* newline */    \
                    1               /* NULL terminator */

// write a work struct to a buffer
int worktobuf(char * buf, const size_t size, char * delim, struct work * work);

// get a line from a file and convert the line to a work struct
int filetowork(FILE * file, char * delim, struct work * work);

// convert a formatted string to a work struct
int linetowork(char * line, char * delim, struct work * work);

#endif
