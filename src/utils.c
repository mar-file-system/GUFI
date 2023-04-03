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



#include <ctype.h>              /* isprint() */
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#if HAVE_ZLIB
#include <zlib.h>
#endif

#include "utils.h"

extern int errno;

volatile int sumlock = 0;
struct sum sumout;
pthread_mutex_t sum_mutex = PTHREAD_MUTEX_INITIALIZER;

// global variable to hold per thread state
struct globalthreadstate gts = {};

int printits(struct input *in, struct work *pwork, struct entry_data *ed, int ptid) {
  char  ffielddelim[2];
  FILE *out;

  out = stdout;
  if (in->output == OUTFILE)
     out = gts.outfd[ptid];

  if (in->dodelim == 0) {
    SNPRINTF(ffielddelim,2," ");
  }
  if (in->dodelim == 2) {
    SNPRINTF(ffielddelim,2,"%s",fielddelim);
  }
  if (in->dodelim == 1) {
    SNPRINTF(ffielddelim,2,"%s",in->delim);
  }

  fprintf(out, "%s%s",             pwork->name,            ffielddelim);
  fprintf(out, "%c%s",             ed->type,               ffielddelim);
  fprintf(out, "%"STAT_ino"%s",    ed->statuso.st_ino,     ffielddelim);
  fprintf(out, "%d%s",             ed->statuso.st_mode,    ffielddelim);
  fprintf(out, "%"STAT_nlink"%s",  ed->statuso.st_nlink,   ffielddelim);
  fprintf(out, "%d%s",             ed->statuso.st_uid,     ffielddelim);
  fprintf(out, "%d%s",             ed->statuso.st_gid,     ffielddelim);
  fprintf(out, "%"STAT_size"%s",   ed->statuso.st_size,    ffielddelim);
  fprintf(out, "%"STAT_bsize"%s",  ed->statuso.st_blksize, ffielddelim);
  fprintf(out, "%"STAT_blocks"%s", ed->statuso.st_blocks,  ffielddelim);
  fprintf(out, "%ld%s",            ed->statuso.st_atime,   ffielddelim);
  fprintf(out, "%ld%s",            ed->statuso.st_mtime,   ffielddelim);
  fprintf(out, "%ld%s",            ed->statuso.st_ctime,   ffielddelim);

/* we need this field even if its not populated for gufi_trace2index */
/*
  if (!strncmp(pwork->type,"l",1)) {
    fprintf(out, "%s%s", ed->linkname,ffielddelim);
  }
*/
  fprintf(out, "%s%s", ed->linkname,ffielddelim);

/* we need this field even if its not populated for gufi_trace2index */
/*
  if (ed->xattrs > 0) {
    //printf("xattr: ");
    fprintf(out,"%s%s",ed->xattr,ffielddelim);
  }
*/
  /* fwrite(ed->xattrs, sizeof(char), ed->xattrs_len, out); */
  fprintf(out,"%s",ffielddelim);

  /* this one is for create tiem which posix doesnt have */
  fprintf(out,"%s", ffielddelim);
  /* moved this to end because we would like to use this for input to gufi_trace2index load from file */
  fprintf(out, "%lld%s", pwork->pinode, ffielddelim);
  fprintf(out, "%d%s", ed->suspect, ffielddelim);
  fprintf(out,"\n");
  return 0;
}

int zeroit(struct sum *summary)
{
  summary->totfiles=0;
  summary->totlinks=0;
  summary->minuid=LLONG_MAX;
  summary->maxuid=LLONG_MIN;
  summary->mingid=LLONG_MAX;
  summary->maxgid=LLONG_MIN;
  summary->minsize=LLONG_MAX;
  summary->maxsize=LLONG_MIN;
  summary->totltk=0;
  summary->totmtk=0;
  summary->totltm=0;
  summary->totmtm=0;
  summary->totmtg=0;
  summary->totmtt=0;
  summary->totsize=0;
  summary->minctime=LLONG_MAX;
  summary->maxctime=LLONG_MIN;
  summary->minmtime=LLONG_MAX;
  summary->maxmtime=LLONG_MIN;
  summary->minatime=LLONG_MAX;
  summary->maxatime=LLONG_MIN;
  summary->minblocks=LLONG_MAX;
  summary->maxblocks=LLONG_MIN;
  summary->setit=0;
  summary->totxattr=0;
  summary->totsubdirs=0;
  summary->maxsubdirfiles=LLONG_MIN;
  summary->maxsubdirlinks=LLONG_MIN;
  summary->maxsubdirsize=LLONG_MIN;
  summary->mincrtime=LLONG_MAX;
  summary->maxcrtime=LLONG_MIN;
  summary->minossint1=LLONG_MAX;
  summary->maxossint1=LLONG_MIN;
  summary->totossint1=0;
  summary->minossint2=LLONG_MAX;
  summary->maxossint2=LLONG_MIN;
  summary->totossint2=0;
  summary->minossint3=LLONG_MAX;
  summary->maxossint3=LLONG_MIN;
  summary->totossint3=0;
  summary->minossint4=LLONG_MAX;
  summary->maxossint4=LLONG_MIN;
  summary->totossint4=0;
  return 0;
}

int sumit(struct sum *summary, struct entry_data *ed) {

  if (summary->setit == 0) {
    summary->minuid=ed->statuso.st_uid;
    summary->maxuid=ed->statuso.st_uid;
    summary->mingid=ed->statuso.st_gid;
    summary->maxgid=ed->statuso.st_gid;
    summary->minsize=ed->statuso.st_size;
    summary->maxsize=ed->statuso.st_size;;
    summary->minctime=ed->statuso.st_ctime;
    summary->maxctime=ed->statuso.st_ctime;
    summary->minmtime=ed->statuso.st_mtime;
    summary->maxmtime=ed->statuso.st_mtime;
    summary->minatime=ed->statuso.st_atime;
    summary->maxatime=ed->statuso.st_atime;;
    summary->minblocks=ed->statuso.st_blocks;
    summary->maxblocks=ed->statuso.st_blocks;
    summary->mincrtime=ed->crtime;
    summary->maxcrtime=ed->crtime;
    summary->minossint1=ed->ossint1;
    summary->maxossint1=ed->ossint1;
    summary->totossint1=ed->ossint1;
    summary->minossint2=ed->ossint2;
    summary->maxossint2=ed->ossint2;
    summary->totossint2=ed->ossint2;
    summary->minossint3=ed->ossint3;
    summary->maxossint3=ed->ossint3;
    summary->totossint3=ed->ossint3;
    summary->minossint4=ed->ossint4;
    summary->maxossint4=ed->ossint4;
    summary->totossint4=ed->ossint4;
    summary->setit=1;
  }
  if (ed->type == 'f') {
     summary->totfiles++;
     if (ed->statuso.st_size < summary->minsize) summary->minsize=ed->statuso.st_size;
     if (ed->statuso.st_size > summary->maxsize) summary->maxsize=ed->statuso.st_size;
     if (ed->statuso.st_size <= 1024) summary->totltk++;
     if (ed->statuso.st_size > 1024) summary->totmtk++;
     if (ed->statuso.st_size <= 1048576) summary->totltm++;
     if (ed->statuso.st_size > 1048576) summary->totmtm++;
     if (ed->statuso.st_size > 1073741824) summary->totmtg++;
     if (ed->statuso.st_size > 1099511627776) summary->totmtt++;
     summary->totsize=summary->totsize+ed->statuso.st_size;

     if (ed->statuso.st_blocks < summary->minblocks) summary->minblocks=ed->statuso.st_blocks;
     if (ed->statuso.st_blocks > summary->maxblocks) summary->maxblocks=ed->statuso.st_blocks;
  }
  if (ed->type == 'l') {
     summary->totlinks++;
  }

  if (ed->statuso.st_uid < summary->minuid) summary->minuid=ed->statuso.st_uid;
  if (ed->statuso.st_uid > summary->maxuid) summary->maxuid=ed->statuso.st_uid;
  if (ed->statuso.st_gid < summary->mingid) summary->mingid=ed->statuso.st_gid;
  if (ed->statuso.st_gid > summary->maxgid) summary->maxgid=ed->statuso.st_gid;
  if (ed->statuso.st_ctime < summary->minctime) summary->minctime=ed->statuso.st_ctime;
  if (ed->statuso.st_ctime > summary->maxctime) summary->maxctime=ed->statuso.st_ctime;
  if (ed->statuso.st_mtime < summary->minmtime) summary->minmtime=ed->statuso.st_mtime;
  if (ed->statuso.st_mtime > summary->maxmtime) summary->maxmtime=ed->statuso.st_mtime;
  if (ed->statuso.st_atime < summary->minatime) summary->minatime=ed->statuso.st_atime;
  if (ed->statuso.st_atime > summary->maxatime) summary->maxatime=ed->statuso.st_atime;
  if (ed->crtime < summary->mincrtime) summary->mincrtime=ed->crtime;
  if (ed->crtime > summary->maxcrtime) summary->maxcrtime=ed->crtime;

  if (ed->ossint1 < summary->minossint1) summary->minossint1=ed->ossint1;
  if (ed->ossint1 > summary->maxossint1) summary->maxossint1=ed->ossint1;
  summary->totossint1=summary->totossint1+ed->ossint1;

  if (ed->ossint2 < summary->minossint2) summary->minossint2=ed->ossint2;
  if (ed->ossint2 > summary->maxossint2) summary->maxossint2=ed->ossint2;
  summary->totossint2=summary->totossint2+ed->ossint2;

  if (ed->ossint3 < summary->minossint3) summary->minossint3=ed->ossint3;
  if (ed->ossint3 > summary->maxossint3) summary->maxossint3=ed->ossint3;
  summary->totossint3=summary->totossint3+ed->ossint3;

  if (ed->ossint4 < summary->minossint4) summary->minossint4=ed->ossint4;
  if (ed->ossint4 > summary->maxossint4) summary->maxossint4=ed->ossint4;
  summary->totossint4=summary->totossint4+ed->ossint4;

  summary->totxattr += ed->xattrs.count;

  return 0;
}

int tsumit(struct sum *sumin, struct sum *smout) {

  pthread_mutex_lock(&sum_mutex);

  /* if sumin totsubdirs is > 0 we are summing a treesummary not a dirsummary */
  if (sumin->totsubdirs > 0) {
    smout->totsubdirs=smout->totsubdirs+sumin->totsubdirs;
    if (sumin->maxsubdirfiles > smout->maxsubdirfiles) smout->maxsubdirfiles = sumin->maxsubdirfiles;
    if (sumin->maxsubdirlinks > smout->maxsubdirlinks) smout->maxsubdirlinks = sumin->maxsubdirlinks;
    if (sumin->maxsubdirsize  > smout->maxsubdirsize)  smout->maxsubdirsize  = sumin->maxsubdirsize;
  } else {
    smout->totsubdirs++;
    if (sumin->totfiles > smout->maxsubdirfiles) smout->maxsubdirfiles = sumin->totfiles;
    if (sumin->totlinks > smout->maxsubdirlinks) smout->maxsubdirlinks = sumin->totlinks;
    if (sumin->totsize  > smout->maxsubdirsize)  smout->maxsubdirsize  = sumin->totsize;
  }

  smout->totfiles += sumin->totfiles;
  smout->totlinks += sumin->totlinks;

  if (sumin->totfiles > 0) {
    if (smout->setit == 0) {
      smout->minuid=sumin->minuid;
      smout->maxuid=sumin->maxuid;
      smout->mingid=sumin->mingid;
      smout->maxgid=sumin->maxgid;
      smout->minsize=sumin->minsize;
      smout->maxsize=sumin->maxsize;
      smout->minctime=sumin->minctime;
      smout->maxctime=sumin->maxctime;
      smout->minmtime=sumin->minmtime;
      smout->maxmtime=sumin->maxmtime;
      smout->minatime=sumin->minatime;
      smout->maxatime=sumin->maxatime;
      smout->minblocks=sumin->minblocks;
      smout->maxblocks=sumin->maxblocks;
      smout->mincrtime=sumin->mincrtime;
      smout->maxcrtime=sumin->maxcrtime;
      smout->minossint1=sumin->minossint1;
      smout->maxossint1=sumin->maxossint1;
      smout->minossint2=sumin->minossint2;
      smout->maxossint2=sumin->maxossint2;
      smout->minossint3=sumin->minossint3;
      smout->maxossint3=sumin->maxossint3;
      smout->minossint4=sumin->minossint4;
      smout->maxossint4=sumin->maxossint4;
      smout->setit=1;
    }

  /* only set these mins and maxes if there are files in the directory
     otherwise mins are all zero */
  //if (sumin->totfiles > 0) {
    if (sumin->minuid < smout->minuid) smout->minuid=sumin->minuid;
    if (sumin->maxuid > smout->maxuid) smout->maxuid=sumin->maxuid;
    if (sumin->mingid < smout->mingid) smout->mingid=sumin->mingid;
    if (sumin->maxgid > smout->maxgid) smout->maxgid=sumin->maxgid;
    if (sumin->minsize < smout->minsize) smout->minsize=sumin->minsize;
    if (sumin->maxsize > smout->maxsize) smout->maxsize=sumin->maxsize;
    if (sumin->minblocks < smout->minblocks) smout->minblocks=sumin->minblocks;
    if (sumin->maxblocks > smout->maxblocks) smout->maxblocks=sumin->maxblocks;
    if (sumin->minctime < smout->minctime) smout->minctime=sumin->minctime;
    if (sumin->maxctime > smout->maxctime) smout->maxctime=sumin->maxctime;
    if (sumin->minmtime < smout->minmtime) smout->minmtime=sumin->minmtime;
    if (sumin->maxmtime > smout->maxmtime) smout->maxmtime=sumin->maxmtime;
    if (sumin->minatime < smout->minatime) smout->minatime=sumin->minatime;
    if (sumin->maxatime > smout->maxatime) smout->maxatime=sumin->maxatime;
    if (sumin->mincrtime < smout->mincrtime) smout->mincrtime=sumin->mincrtime;
    if (sumin->maxcrtime > smout->maxcrtime) smout->maxcrtime=sumin->maxcrtime;
    if (sumin->minossint1 < smout->minossint1) smout->minossint1=sumin->minossint1;
    if (sumin->maxossint1 > smout->maxossint1) smout->maxossint1=sumin->maxossint1;
    if (sumin->minossint2 < smout->minossint2) smout->minossint2=sumin->minossint2;
    if (sumin->maxossint2 > smout->maxossint2) smout->maxossint2=sumin->maxossint2;
    if (sumin->minossint3 < smout->minossint3) smout->minossint3=sumin->minossint3;
    if (sumin->maxossint3 > smout->maxossint3) smout->maxossint3=sumin->maxossint3;
    if (sumin->minossint4 < smout->minossint4) smout->minossint4=sumin->minossint4;
    if (sumin->maxossint4 > smout->maxossint4) smout->maxossint4=sumin->maxossint4;
  }

  smout->totltk     += sumin->totltk;
  smout->totmtk     += sumin->totmtk;
  smout->totltm     += sumin->totltm;
  smout->totmtm     += sumin->totmtm;
  smout->totmtg     += sumin->totmtg;
  smout->totmtt     += sumin->totmtt;
  smout->totsize    += sumin->totsize;
  smout->totxattr   += sumin->totxattr;
  smout->totossint1 += sumin->totossint1;
  smout->totossint2 += sumin->totossint2;
  smout->totossint3 += sumin->totossint3;
  smout->totossint4 += sumin->totossint4;

  pthread_mutex_unlock(&sum_mutex);

  return 0;
}

// given a possibly-multi-level path of directories (final component is
// also a dir), create the parent dirs all the way down.
//
int mkpath(char *path, mode_t mode, uid_t uid, gid_t gid) {
  for (char *p=strchr(path+1, '/'); p; p=strchr(p+1, '/')) {
    *p='\0';
    if (mkdir(path, mode)==-1) {
      if (errno!=EEXIST) {
         *p='/';
         return -1;
      }
    }
    else {
      chmod(path, mode);
      chown(path, uid, gid);
    }
    *p='/';
  }
  return mkdir(path,mode);
}

int dupdir(const char *path, struct stat *stat)
{
    /* if dupdir is called, doing a memory copy will not be relatively heavyweight */
    const size_t path_len = strlen(path);
    char copy[MAXPATH];
    SNFORMAT_S(copy, sizeof(copy), 1,
               path, path_len);
    copy[path_len] = '\0';

    // the writer must be able to create the index files into this directory so or in S_IWRITE
    if (mkdir(copy, stat->st_mode) != 0) {
      if (errno == ENOENT) {
        mkpath(copy, stat->st_mode, stat->st_uid, stat->st_gid);
      } else if (errno != EEXIST) {
        return 1;
      }
    }
    chmod(copy, stat->st_mode);
    chown(copy, stat->st_uid, stat->st_gid);
    // we dont need to set xattrs/time on the gufi directory those are in the db
    // the gufi directory structure is there only to walk, not to provide
    // information, the information is in the db

    return 0;
}

int shortpath(const char *name, char *nameout, char *endname) {
     char prefix[MAXPATH];
     char *pp;
     size_t i;
     int slashfound;

     *endname = 0;              // in case there's no '/'
     SNPRINTF(prefix, MAXPATH, "%s", name);
     i = strlen(prefix);
     pp = prefix+i;
     slashfound = 0;
     while (i > 0) {
       if (!strncmp(pp,"/",1)) {
          memset(pp, 0, 1);
          SNPRINTF(endname, MAXPATH, "%s", pp+1);
          slashfound = 1;
          break;
       }
       pp--;
       i--;
     }
     if (slashfound == 0) {
        SNPRINTF(endname, MAXPATH, "%s", name);
        memset(nameout, 0, 1);
     } else
        SNPRINTF(nameout, MAXPATH, "%s", prefix);
     return 0;
}

int SNPRINTF(char *str, size_t size, const char *format, ...) {
    va_list args;
    va_start(args, format);
    const int n = vsnprintf(str, size, format, args);
    va_end(args);
    if (n < 0) {
        fprintf(stderr, "%s:%d Error printing %s\n",
                __FILE__, __LINE__, format);
    }
    if ((size_t) n >= size) {
        fprintf(stderr, "%s:%d Warning: Message "
                "was truncated to %d characters: %s\n",
                __FILE__, __LINE__, n, str);
    }
    return n;
}

/* Equivalent to snprintf printing only strings. Variadic arguments
   should be pairs of strings and their lengths (to try to prevent
   unnecessary calls to strlen). Make sure to typecast the lengths
   to size_t or weird bugs may occur */
size_t SNFORMAT_S(char *dst, const size_t dst_len, size_t count, ...) {
    size_t max_len = dst_len - 1;

    va_list args;
    va_start(args, count);
    for(size_t i = 0; i < count; i++) {
        const char *src = va_arg(args, char *);
        /* size_t does not work, but solution found at */
        /* https://stackoverflow.com/a/12864069/341683 */
        /* does not seem to fix it either */
        const size_t len = va_arg(args, unsigned int);
        const size_t copy_len = (len < max_len)?len:max_len;
        memcpy(dst, src, copy_len);
        dst += copy_len;
        max_len -= copy_len;
    }
    va_end(args);

    *dst = '\0';
    return dst_len - max_len - 1;
}

#if HAVE_ZLIB
/*
 * not static to allow for these globals and
 * functions to be declared for testing
 */

/* starting offset of actual struct work data */
const size_t WORK_OFFSET =
    ((size_t) &((struct work *) NULL)->root);

/* size of struct work without the compression information */
const size_t WORK_LEN =
    sizeof(struct work) - WORK_OFFSET;

const size_t MAXIMUM_COMPRESSED_LEN = WORK_LEN;

/* dst is a reference to a pointer on the heap */
void compress_work_zlib(struct work **dst, struct work *src, const size_t max_len) {
    /* compress work item */
    (*dst)->len = WORK_LEN;
    const int ret = compress((unsigned char *) &(*dst)->root, &(*dst)->len,
                             (unsigned char *) &src->root, WORK_LEN);
    (*dst)->compressed = (ret == Z_OK);

    /* if compressed enough, reallocate to reduce memory usage */
    if ((*dst)->compressed && ((*dst)->len < max_len)) {
        struct work *r = realloc(*dst, WORK_OFFSET + (*dst)->len);
        if (r) {
            *dst = r;
        }
        /* if realloc failed, dst can still be used */
    }
}

void decompress_work_zlib(struct work **dst, struct work *src, void *data) {
    struct work *work = (struct work *) data;
    if (work->compressed) {
        size_t work_len = WORK_LEN;
        uncompress((unsigned char *) &src->root, &work_len,
                   (unsigned char *) &work->root, work->len);
        free(data);

        *dst = src;
    }
    else {
        *dst = work;
    }
}

void free_work_zlib(struct work *heap, struct work *stack) {
    if (heap != stack) {
        free(heap);
    }
}
#endif

int enqueue_work(const int comp, struct work *stack,
                 QPTPool_t *ctx, const size_t id, QPTPoolFunc_t processdir) {
    struct work *copy = malloc(sizeof(*stack));

    #if HAVE_ZLIB
    if (comp) {
        compress_work_zlib(&copy, stack, MAXIMUM_COMPRESSED_LEN);
        if (copy->compressed) {
            QPTPool_enqueue(ctx, id, processdir, copy);
            return 0;
        }
    }
    #else
    (void) comp;
    #endif

    memcpy(copy, stack, sizeof(*stack));

    QPTPool_enqueue(ctx, id, processdir, copy);

    return 0;
}

int dequeue_work(const int comp, void *data,
                 struct work **heap, struct work *stack) {
    #if HAVE_ZLIB
    if (comp) {
        decompress_work_zlib(heap, stack, data);
        return 0;
    }
    #else
    (void) comp;
    (void) stack;
    #endif

    *heap = (struct work *) data;

    return 0;
}

int free_work(const int comp, struct work *heap, struct work *stack) {
    #if HAVE_ZLIB
    if (comp) {
        free_work_zlib(heap, stack);
        return 0;
    }
    #else
    (void) comp;
    (void) stack;
    #endif
    if (heap->recursion_level == 0) {
        free(heap);
    }

    return 0;
}

/*
 * Push the subdirectories in the current directory onto the queue
 * and process non directories using a user provided function.
 *
 * work->statuso should be filled before calling descend.
 *
 * If stat_entries is set to 0, the processing thread should stat
 * the path. Nothing in work->statuso will be filled except the
 * entry type. Links will not be read.
 *
 * If work->recursion_level > 0, the work item that is passed
 * into the processdir function will be allocated on the stack
 * instead of on the heap, so do not free it.
 */
int descend(QPTPool_t *ctx, const size_t id, void *args,
            struct input *in, struct work *work, ino_t inode,
            DIR *dir, trie_t *skip_names, const int skip_db,
            const int stat_entries, QPTPoolFunc_t processdir,
            processnondir_f processnondir, void *nondir_args,
            size_t *dir_count, size_t *dirs_insitu, size_t *nondir_count, size_t *nondirs_processed) {
    if (!work) {
        return 1;
    }

    size_t dirs = 0;
    size_t insitu = 0;
    size_t nondirs = 0;
    size_t processed = 0;

    if (work->level < in->max_level) {
        /* calculate once */
        const size_t next_level = work->level + 1;
        const size_t recursion_level = work->recursion_level + 1;

        struct dirent *dir_child = NULL;
        while ((dir_child = readdir(dir))) {
            const size_t len = strlen(dir_child->d_name);

            /* skip . and .. and *.db */
            const int skip = (trie_search(skip_names, dir_child->d_name, len) ||
                              (skip_db && (strncmp(dir_child->d_name + len - 3, ".db", 3) == 0)));
            if (skip) {
                continue;
            }

            struct work child;
            memset(&child, 0, sizeof(child));

            struct entry_data child_ed;
            memset(&child_ed, 0, sizeof(child_ed));

            /* get child path */
            child.name_len = SNFORMAT_S(child.name, MAXPATH, 3,
                                        work->name, work->name_len,
                                        "/", (size_t) 1,
                                        dir_child->d_name, len);
            child.basename_len = len;
            child.level = next_level;
            child.root = work->root;
            child.root_len = work->root_len;
            child.pinode = inode;

            if (stat_entries) {
                /* get the child's metadata */
                if (lstat(child.name, &child_ed.statuso) < 0) {
                    continue;
                }
            }
            else {
                /* only get the child's type - processing thread should stat */
                switch (dir_child->d_type) {
                    case DT_DIR:
                        child_ed.statuso.st_mode = S_IFDIR;
                        break;
                    case DT_LNK:
                        child_ed.statuso.st_mode = S_IFLNK;
                        break;
                    case DT_REG:
                        child_ed.statuso.st_mode = S_IFREG;
                        break;
                    default:
                        break;
                }
            }

            /* push subdirectories onto the queue */
            if (S_ISDIR(child_ed.statuso.st_mode)) {
                child_ed.type = 'd';

                if (!in->subdir_limit || (dirs < in->subdir_limit)) {
                    enqueue_work(in->compress, &child, ctx, id, processdir);
                }
                else {
                    /*
                     * If this directory has too many subdirectories,
                     * process the current subdirectory here instead
                     * of enqueuing it. This only allows for one
                     * subdirectory work item to be allocated at a
                     * time instead of all of them, reducing overall
                     * memory usage. This branch is only applied at
                     * this level, so small subdirectories will still
                     * enqueue work, and large subdirectories will
                     * still enqueue some work and process the
                     * remaining in-situ.
                     *
                     * Return value should probably be used.
                     */
                    child.recursion_level = recursion_level;
                    processdir(ctx, id, &child, args);
                    insitu++;
                }

                dirs++;

                continue;
            }
            /* non directories */
            else if (S_ISLNK(child_ed.statuso.st_mode)) {
                child_ed.type = 'l';
                if (stat_entries) {
                    readlink(child.name, child_ed.linkname, MAXPATH);
                }
            }
            else if (S_ISREG(child_ed.statuso.st_mode)) {
                child_ed.type = 'f';
            }
            else {
                /* other types are not stored */
                continue;
            }

            nondirs++;

            if (processnondir) {
                if (in->external_enabled) {
                    xattrs_setup(&child_ed.xattrs);
                    xattrs_get(child.name, &child_ed.xattrs);
                }

                processnondir(&child, &child_ed, nondir_args);
                processed++;

                if (in->external_enabled) {
                    xattrs_cleanup(&child_ed.xattrs);
                }
            }
        }
    }

    if (dir_count) {
        *dir_count = dirs;
    }

    if (dirs_insitu) {
        *dirs_insitu = insitu;
    }

    if (nondir_count) {
        *nondir_count = nondirs;
    }

    if (nondirs_processed) {
        *nondirs_processed = processed;
    }

    return 0;
}

/* convert a mode to a human readable string */
char *modetostr(char *str, const size_t size, const mode_t mode)
{
    if (size < 11) {
        return NULL;
    }

    if (str) {
        SNPRINTF(str, size, "----------");
        if (S_ISDIR(mode))  str[0] = 'd';
        if (S_ISLNK(mode))  str[0] = 'l';
        if (mode & S_IRUSR) str[1] = 'r';
        if (mode & S_IWUSR) str[2] = 'w';
        if (mode & S_IXUSR) str[3] = 'x';
        if (mode & S_IRGRP) str[4] = 'r';
        if (mode & S_IWGRP) str[5] = 'w';
        if (mode & S_IXGRP) str[6] = 'x';
        if (mode & S_IROTH) str[7] = 'r';
        if (mode & S_IWOTH) str[8] = 'w';
        if (mode & S_IXOTH) str[9] = 'x';
    }

    return str;
}

static int loop_matches(const char c, const char *match, const size_t match_count) {
    for(size_t i = 0; i < match_count; i++) {
        if (match[i] == c) {
            return 1;
        }
    }

    return 0;
}

size_t trailing_match_index(const char *str, size_t len,
                            const char *match, const size_t match_count) {
    if (!str) {
        return 0;
    }

    while (len && loop_matches(str[len - 1], match, match_count)) {
        len--;
    }

    return len;
}

size_t trailing_non_match_index(const char *str, size_t len,
                                const char *not_match, const size_t not_match_count) {
    if (!str) {
        return 0;
    }

    while (len && !loop_matches(str[len - 1], not_match, not_match_count)) {
        len--;
    }

    return len;
}

/*
 * find first slash before the basename a.k.a. get the dirname length
 *
 * /      -> /
 * /a/b/c -> /a/b/
 *
 * This function is meant to be called during input processing to get
 * the parent of each input directory. The input paths should not have
 * trailing slashes.
 *
 * This function allows for the index to be created in a directory
 * under the provided root, instead of directly in the provided
 * root. If the source directory is the root directory, this still
 * makes sense.
 *
 * toname = /home/search
 * source = /a/b/c
 * result = /home/search/c
 * source = /
 * result = /home/search/
 */
size_t dirname_len(const char *path, size_t len) {
    return trailing_non_match_index(path, len, "/", 1);
}

/* create a trie of directory names to skip from a file */
int setup_directory_skip(const char *filename, trie_t **skip) {
    if (!skip) {
        return -1;
    }

    *skip = trie_alloc();
    if (!*skip) {
        fprintf(stderr, "Error: Could not set up skip list\n");
        return -1;
    }

    /* always skip . and .. */
    trie_insert(*skip, ".",  1);
    trie_insert(*skip, "..", 2);

    /* add user defined directory names to skip */
    if (filename && strlen(filename)) {
        FILE *skipfile = fopen(filename, "r");
        if (!skipfile) {
            fprintf(stderr, "Error: Cannot open skip file \"%s\"\n", filename);
            trie_free(*skip);
            *skip = NULL;
            return -1;
        }

        /* read line */
        char line[MAXPATH];
        while (fgets(line, MAXPATH, skipfile) == line) {
            /* only keep first word */
            char name[MAXPATH];
            if (sscanf(line, "%s", name) != 1) {
                continue;
            }

            trie_insert(*skip, name, strlen(name));
        }

        fclose(skipfile);
    }

    return 0;
}

/* strstr/strtok replacement */
/* does not terminate on NULL character */
/* does not skip to the next non-empty column */
char *split(char *src, const char *delim, const char *end) {
    if (!src || !delim || !end || (src > end)) {
        return NULL;
    }

    while (src < end) {
        for(const char *d = delim; *d; d++) {
            if (*src == *d) {
                *src = '\x00';
                return src + 1;
            }
        }

        src++;
    }

    return NULL;
}
