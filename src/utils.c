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
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils.h"

extern int errno;

volatile int sumlock = 0;
struct sum sumout;
pthread_mutex_t sum_mutex = PTHREAD_MUTEX_INITIALIZER;

// global variable to hold per thread state
struct globalthreadstate gts = {};

int printits(struct work *pwork, int ptid) {
  char  ffielddelim[2];
  FILE *out;

  out = stdout;
  if (in.output == OUTFILE)
     out = gts.outfd[ptid];

  if (in.dodelim == 0) {
    SNPRINTF(ffielddelim,2," ");
  }
  if (in.dodelim == 2) {
    SNPRINTF(ffielddelim,2,"%s",fielddelim);
  }
  if (in.dodelim == 1) {
    SNPRINTF(ffielddelim,2,"%s",in.delim);
  }

  fprintf(out, "%s%s",             pwork->name,               ffielddelim);
  fprintf(out, "%c%s",             pwork->type[0],            ffielddelim);
  fprintf(out, "%"STAT_ino"%s",    pwork->statuso.st_ino,     ffielddelim);
  fprintf(out, "%d%s",             pwork->statuso.st_mode,    ffielddelim);
  fprintf(out, "%"STAT_nlink"%s",  pwork->statuso.st_nlink,   ffielddelim);
  fprintf(out, "%d%s",             pwork->statuso.st_uid,     ffielddelim);
  fprintf(out, "%d%s",             pwork->statuso.st_gid,     ffielddelim);
  fprintf(out, "%"STAT_size"%s",   pwork->statuso.st_size,    ffielddelim);
  fprintf(out, "%"STAT_bsize"%s",  pwork->statuso.st_blksize, ffielddelim);
  fprintf(out, "%"STAT_blocks"%s", pwork->statuso.st_blocks,  ffielddelim);
  fprintf(out, "%ld%s",            pwork->statuso.st_atime,   ffielddelim);
  fprintf(out, "%ld%s",            pwork->statuso.st_mtime,   ffielddelim);
  fprintf(out, "%ld%s",            pwork->statuso.st_ctime,   ffielddelim);

/* we need this field even if its not populated for gufi_trace2index */
/*
  if (!strncmp(pwork->type,"l",1)) {
    fprintf(out, "%s%s", pwork->linkname,ffielddelim);
  }
*/
  fprintf(out, "%s%s", pwork->linkname,ffielddelim);

/* we need this field even if its not populated for gufi_trace2index */
/*
  if (pwork->xattrs > 0) {
    //printf("xattr: ");
    fprintf(out,"%s%s",pwork->xattr,ffielddelim);
  }
*/
  /* fwrite(pwork->xattrs, sizeof(char), pwork->xattrs_len, out); */
  fprintf(out,"%s",ffielddelim);

  /* this one is for create tiem which posix doesnt have */
  fprintf(out,"%s", ffielddelim);
  /* moved this to end because we would like to use this for input to gufi_trace2index load from file */
  fprintf(out, "%lld%s", pwork->pinode, ffielddelim);
  fprintf(out, "%d%s", pwork->suspect, ffielddelim);
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

int sumit(struct sum *summary, struct work *pwork) {

  if (summary->setit == 0) {
    summary->minuid=pwork->statuso.st_uid;
    summary->maxuid=pwork->statuso.st_uid;
    summary->mingid=pwork->statuso.st_gid;
    summary->maxgid=pwork->statuso.st_gid;
    summary->minsize=pwork->statuso.st_size;
    summary->maxsize=pwork->statuso.st_size;;
    summary->minctime=pwork->statuso.st_ctime;
    summary->maxctime=pwork->statuso.st_ctime;
    summary->minmtime=pwork->statuso.st_mtime;
    summary->maxmtime=pwork->statuso.st_mtime;
    summary->minatime=pwork->statuso.st_atime;
    summary->maxatime=pwork->statuso.st_atime;;
    summary->minblocks=pwork->statuso.st_blocks;
    summary->maxblocks=pwork->statuso.st_blocks;
    summary->mincrtime=pwork->crtime;
    summary->maxcrtime=pwork->crtime;
    summary->minossint1=pwork->ossint1;
    summary->maxossint1=pwork->ossint1;
    summary->totossint1=pwork->ossint1;
    summary->minossint2=pwork->ossint2;
    summary->maxossint2=pwork->ossint2;
    summary->totossint2=pwork->ossint2;
    summary->minossint3=pwork->ossint3;
    summary->maxossint3=pwork->ossint3;
    summary->totossint3=pwork->ossint3;
    summary->minossint4=pwork->ossint4;
    summary->maxossint4=pwork->ossint4;
    summary->totossint4=pwork->ossint4;
    summary->setit=1;
  }
  if (!strncmp(pwork->type,"f",1)) {
     summary->totfiles++;
     if (pwork->statuso.st_size < summary->minsize) summary->minsize=pwork->statuso.st_size;
     if (pwork->statuso.st_size > summary->maxsize) summary->maxsize=pwork->statuso.st_size;
     if (pwork->statuso.st_size <= 1024) summary->totltk++;
     if (pwork->statuso.st_size > 1024) summary->totmtk++;
     if (pwork->statuso.st_size <= 1048576) summary->totltm++;
     if (pwork->statuso.st_size > 1048576) summary->totmtm++;
     if (pwork->statuso.st_size > 1073741824) summary->totmtg++;
     if (pwork->statuso.st_size > 1099511627776) summary->totmtt++;
     summary->totsize=summary->totsize+pwork->statuso.st_size;

     if (pwork->statuso.st_blocks < summary->minblocks) summary->minblocks=pwork->statuso.st_blocks;
     if (pwork->statuso.st_blocks > summary->maxblocks) summary->maxblocks=pwork->statuso.st_blocks;
  }
  if (!strncmp(pwork->type,"l",1)) {
     summary->totlinks++;
  }

  if (pwork->statuso.st_uid < summary->minuid) summary->minuid=pwork->statuso.st_uid;
  if (pwork->statuso.st_uid > summary->maxuid) summary->maxuid=pwork->statuso.st_uid;
  if (pwork->statuso.st_gid < summary->mingid) summary->mingid=pwork->statuso.st_gid;
  if (pwork->statuso.st_gid > summary->maxgid) summary->maxgid=pwork->statuso.st_gid;
  if (pwork->statuso.st_ctime < summary->minctime) summary->minctime=pwork->statuso.st_ctime;
  if (pwork->statuso.st_ctime > summary->maxctime) summary->maxctime=pwork->statuso.st_ctime;
  if (pwork->statuso.st_mtime < summary->minmtime) summary->minmtime=pwork->statuso.st_mtime;
  if (pwork->statuso.st_mtime > summary->maxmtime) summary->maxmtime=pwork->statuso.st_mtime;
  if (pwork->statuso.st_atime < summary->minatime) summary->minatime=pwork->statuso.st_atime;
  if (pwork->statuso.st_atime > summary->maxatime) summary->maxatime=pwork->statuso.st_atime;
  if (pwork->crtime < summary->mincrtime) summary->mincrtime=pwork->crtime;
  if (pwork->crtime > summary->maxcrtime) summary->maxcrtime=pwork->crtime;

  if (pwork->ossint1 < summary->minossint1) summary->minossint1=pwork->ossint1;
  if (pwork->ossint1 > summary->maxossint1) summary->maxossint1=pwork->ossint1;
  summary->totossint1=summary->totossint1+pwork->ossint1;

  if (pwork->ossint2 < summary->minossint2) summary->minossint2=pwork->ossint2;
  if (pwork->ossint2 > summary->maxossint2) summary->maxossint2=pwork->ossint2;
  summary->totossint2=summary->totossint2+pwork->ossint2;

  if (pwork->ossint3 < summary->minossint3) summary->minossint3=pwork->ossint3;
  if (pwork->ossint3 > summary->maxossint3) summary->maxossint3=pwork->ossint3;
  summary->totossint3=summary->totossint3+pwork->ossint3;

  if (pwork->ossint4 < summary->minossint4) summary->minossint4=pwork->ossint4;
  if (pwork->ossint4 > summary->maxossint4) summary->maxossint4=pwork->ossint4;
  summary->totossint4=summary->totossint4+pwork->ossint4;

  summary->totxattr += pwork->xattrs.count;

  return 0;
}

int tsumit(struct sum *sumin,struct sum *smout) {

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
    //printf("mkpath mkdir file_path %s p %s\n", file_path,p);
    *p='\0';
    //printf("mkpath mkdir file_path %s\n", file_path);
    //if (mkdir(file_path, mode)==-1) {
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
  //printf("mkpath mkdir sp %s\n",sp);
  return mkdir(path,mode);
}

int dupdir(char *path, struct stat *stat)
{
    //printf("mkdir %s\n",path);
    // the writer must be able to create the index files into this directory so or in S_IWRITE
    //rc = mkdir(path,pwork->statuso.st_mode | S_IWRITE);
    if (mkdir(path,stat->st_mode) != 0) {
      //perror("mkdir");
      if (errno == ENOENT) {
        //printf("calling mkpath on %s\n",path);
        mkpath(path, stat->st_mode, stat->st_uid, stat->st_gid);
      } else if (errno != EEXIST) {
        return 1;
      }
    }
    chmod(path, stat->st_mode);
    chown(path, stat->st_uid, stat->st_gid);
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
     i=0;
     SNPRINTF(prefix,MAXPATH,"%s",name);
     i=strlen(prefix);
     pp=prefix+i;
     //printf("cutting name down %s len %d\n",prefix,i);
     slashfound=0;
     while (i > 0) {
       if (!strncmp(pp,"/",1)) {
          memset(pp, 0, 1);
          sprintf(endname,"%s",pp+1);
          slashfound=1;
          break;
       }
       pp--;
       i--;
     }
     if (slashfound == 0) {
        sprintf(endname,"%s",name);
        bzero(nameout,1);
        //printf("shortpath: name %s, nameout %s, endname %s.\n",name,nameout,endname);
     } else
        sprintf(nameout,"%s",prefix);
     return 0;
}

int printit(const char *name, const struct stat *status, char *type, char *linkname, struct xattrs *xattrs, int printing, long long pinode) {
  if (!printing) return 0;
  printf("%c ", type[0]);

  printf("%s ", name);
  if (!strncmp(type,"l",1)) printf("-> %s ",linkname);

  printf("%"STAT_ino" ",    status->st_ino);
  printf("%lld ",           pinode);
  printf("%d ",             status->st_mode);
  printf("%"STAT_nlink" ",  status->st_nlink);
  printf("%d ",             status->st_uid);
  printf("%d ",             status->st_gid);
  printf("%"STAT_size" ",   status->st_size);
  printf("%"STAT_bsize" ",  status->st_blksize);
  printf("%"STAT_blocks" ", status->st_blocks);
  printf("%ld ",            status->st_atime);
  printf("%ld ",            status->st_mtime);
  printf("%ld ",            status->st_ctime);

  if (xattrs->count) {
    printf("xattr:");
    for(size_t i = 0; i < xattrs->count; i++) {
      printf("%s\\0", xattrs->pairs[i].name);
    }
  }
  printf("\n");
  return 0;
}

int printload(const char *name, const struct stat *status, char *type, char *linkname, struct xattrs *xattrs, long long pinode, char *sortf, FILE *of) {
  fprintf(of,"%s%s",             name,in.delim);
  fprintf(of,"%c%s",             type[0],in.delim);
  fprintf(of,"%"STAT_ino"%s",    status->st_ino,in.delim);
  //fprintf(of,"%lld%s", pinode,in.delim);
  fprintf(of,"%d%s",             status->st_mode,in.delim);
  fprintf(of,"%"STAT_nlink"%s",  status->st_nlink,in.delim);
  fprintf(of,"%d%s",             status->st_uid,in.delim);
  fprintf(of,"%d%s",             status->st_gid,in.delim);
  fprintf(of,"%"STAT_size"%s",   status->st_size,in.delim);
  fprintf(of,"%"STAT_bsize"%s",  status->st_blksize,in.delim);
  fprintf(of,"%"STAT_blocks"%s", status->st_blocks,in.delim);
  fprintf(of,"%ld%s",            status->st_atime,in.delim);
  fprintf(of,"%ld%s",            status->st_mtime,in.delim);
  fprintf(of,"%ld%s",            status->st_ctime,in.delim);
  fprintf(of,"%s%s",             linkname,in.delim);
  for(size_t i = 0; i < xattrs->count; i++) {
      fprintf(of, "%s\\0", xattrs->pairs[i].name);
  }
  fprintf(of,"%s",in.delim);
  /* this one is for create time which posix doenst have */
  fprintf(of,"%s",in.delim);
  /* sort field at the end not required */
  if (strlen(sortf) > 0) fprintf(of,"%s",sortf);

  fprintf(of,"\n");
  return(0);
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

/*
 * Push the subdirectories in the current directory onto the queue
 * and process non directories using a user provided function.
 *
 * work->statuso should be filled before calling descend.
 *
 * If stat_entries is set to 0, the processing thread should stat
 * the path. Nothing in work->statuso will be filled except the
 * entry type. Links will not be read.
 */
int descend(QPTPool_t *ctx, const size_t id,
            struct work *work, DIR *dir, trie_t *skip_names, const int skip_db,
            const int stat_entries, QPTPoolFunc_t processdir,
            int (*process_nondir)(struct work *nondir, void *args), void *args,
            size_t *dir_count, size_t *nondir_count, size_t *nondirs_processed) {
    if (!work) {
        return 1;
    }

    size_t dirs = 0;
    size_t nondirs = 0;
    size_t processed = 0;

    if (work->level < in.max_level) {
        /* calculate once */
        const size_t next_level = work->level + 1;

        struct dirent *dir_child = NULL;
        while ((dir_child = readdir(dir))) {
            const size_t len = strlen(dir_child->d_name);

            /* skip . and .. and *.db */
            const int skip = (trie_search(skip_names, dir_child->d_name, len) ||
                              (skip_db && (strncmp(dir_child->d_name + len - 3, ".db", 3) == 0)));
            if (skip) {
                continue;
            }

            /* get child path */
            struct work child;
            memset(&child, 0, sizeof(struct work));
            child.name_len = SNFORMAT_S(child.name, MAXPATH, 3,
                                        work->name, work->name_len,
                                        "/", (size_t) 1,
                                        dir_child->d_name, len);
            child.basename_len = len;
            child.level = next_level;
            child.root = work->root;
            child.root_len = work->root_len;
            child.pinode = work->statuso.st_ino;

            if (stat_entries) {
                /* get the child's metadata */
                if (lstat(child.name, &child.statuso) < 0) {
                    continue;
                }
            }
            else {
                /* only get the child's type - processing thread should stat */
                switch (dir_child->d_type) {
                    case DT_DIR:
                        child.statuso.st_mode = S_IFDIR;
                        break;
                    case DT_LNK:
                        child.statuso.st_mode = S_IFLNK;
                        break;
                    case DT_REG:
                        child.statuso.st_mode = S_IFREG;
                        break;
                }
            }

            /* push subdirectories onto the queue */
            if (S_ISDIR(child.statuso.st_mode)) {
                child.type[0] = 'd';

                /* make a copy here so that the data can be pushed into the queue */
                /* this is more efficient than malloc+free for every single child */
                struct work *copy = (struct work *) calloc(1, sizeof(struct work));
                memcpy(copy, &child, sizeof(struct work));

                QPTPool_enqueue(ctx, id, processdir, copy);

                dirs++;

                continue;
            }
            /* non directories */
            else if (S_ISLNK(child.statuso.st_mode)) {
                child.type[0] = 'l';
                if (stat_entries) {
                    readlink(child.name, child.linkname, MAXPATH);
                }
            }
            else if (S_ISREG(child.statuso.st_mode)) {
                child.type[0] = 'f';
            }
            else {
                /* other types are not stored */
                continue;
            }

            nondirs++;

            if (process_nondir) {
                if (in.external_enabled) {
                    xattrs_setup(&child.xattrs);
                    xattrs_get(child.name, &child.xattrs);
                }

                processed += !process_nondir(&child, args);

                if (in.external_enabled) {
                    xattrs_cleanup(&child.xattrs);
                }
            }
        }
    }

    if (dir_count) {
        *dir_count = dirs;
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
        FILE *skipfile = fopen(filename, "ro");
        if (!skipfile) {
            fprintf(stderr, "Error: Cannot open skip file %s\n", filename);
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
