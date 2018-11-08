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



#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>              /* isprint() */
#include <errno.h>
#include <limits.h>

#include "utils.h"
#include "structq.h"


// --- threading
threadpool mythpool;

volatile int runningthreads = 0;
pthread_mutex_t running_mutex = PTHREAD_MUTEX_INITIALIZER;

volatile int queuelock = 0;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

volatile int sumlock = 0;
struct sum sumout;
pthread_mutex_t sum_mutex = PTHREAD_MUTEX_INITIALIZER;

// global variable to hold per thread state
struct globalthreadstate gts = {0};






#if __linux__  // GNU/Intel/IBM/etc compilers
#  include <sys/types.h>
#  include <attr/xattr.h>

#  define LISTXATTR(PATH, BUF, SIZE)        llistxattr((PATH), (BUF), (SIZE))
#  define GETXATTR(PATH, KEY, BUF, SIZE)    lgetxattr((PATH), (KEY), (BUF), (SIZE))
#  define SETXATTR(PATH, KEY, VALUE, SIZE)  lsetxattr((PATH), (KEY), (VALUE), (SIZE), 0)

#else  // was: BSDXATTRS  (OSX)
#  include <sys/xattr.h>

#  define LISTXATTR(PATH, BUF, SIZE)        listxattr((PATH), (BUF), (SIZE), XATTR_NOFOLLOW)
#  define GETXATTR(PATH, KEY, BUF, SIZE)    getxattr((PATH), (KEY), (BUF), (SIZE), 0, XATTR_NOFOLLOW)
#  define SETXATTR(PATH, KEY, VALUE, SIZE)  setxattr((PATH), (KEY), (VALUE), (SIZE), 0, 0)
#endif




int printits(struct work *pwork,int ptid) {
  FILE * out;

  out = stdout;
  if (in.outfile > 0)
     out = gts.outfd[ptid];

  fprintf(out,"%s%s",pwork->name,in.delim);

  if (!strncmp(pwork->type,"l",1)) fprintf(out,"l%s",in.delim);
  if (!strncmp(pwork->type,"f",1)) fprintf(out,"f%s",in.delim);
  if (!strncmp(pwork->type,"d",1)) fprintf(out,"d%s",in.delim);

  fprintf(out, "%lld%s", pwork->statuso.st_ino, in.delim);
  fprintf(out, "%lld%s", pwork->pinode,         in.delim);
  fprintf(out, "%d%s",   pwork->statuso.st_mode, in.delim);
  fprintf(out, "%d%s",   pwork->statuso.st_nlink, in.delim);
  fprintf(out, "%d%s",   pwork->statuso.st_uid, in.delim);
  fprintf(out, "%d%s",   pwork->statuso.st_gid, in.delim);
  fprintf(out, "%lld%s", pwork->statuso.st_size, in.delim);
  fprintf(out, "%d%s",   pwork->statuso.st_blksize, in.delim);
  fprintf(out, "%lld%s", pwork->statuso.st_blocks, in.delim);
  fprintf(out, "%ld%s",  pwork->statuso.st_atime, in.delim);
  fprintf(out, "%ld%s",  pwork->statuso.st_mtime, in.delim);
  fprintf(out, "%ld%s",  pwork->statuso.st_ctime, in.delim);
  fprintf(out, "%d%s",  pwork->suspect, in.delim);

  if (!strncmp(pwork->type,"l",1)) {
    fprintf(out, "%s", pwork->linkname);
    fprintf(out, "%s", in.delim);
  }
  if (pwork->xattrs > 0) {
    //printf("xattr: ");
    fprintf(out,"%s",pwork->xattr);
  }
  fprintf(out,"%s\n", in.delim);
  return 0;
}


int pullxattrs( const char *name, char *bufx) {
    char buf[MAXXATTR];
    char bufv[MAXXATTR];
    char * key;
    int keylen;
    ssize_t buflen;
    ssize_t bufvlen;
    char *bufxp;
    int xattrs;
    unsigned int ptest;
    bufxp=bufx;
    bzero(buf,sizeof(buf));

    buflen = LISTXATTR(name, buf, sizeof(buf));

    xattrs=0;

    if (buflen > 0) {
       //printf("xattr exists len %zu %s\n",buflen,buf);
       key=buf;
       while (buflen > 0) {
         bzero(bufv,sizeof(bufv));

         bufvlen = GETXATTR(name, key, bufv, sizeof(bufv));

         keylen=strlen(key) + 1;
         //printf("key: %s value: %s len %zd keylen %d\n",key,bufv,bufvlen,keylen);
         sprintf(bufxp,"%s%s",key,xattrdelim); bufxp=bufxp+keylen;
         ptest = *(bufv);
         if (isprint(ptest)) {
           sprintf(bufxp,"%s%s",bufv,xattrdelim);
           bufxp=bufxp+bufvlen+1;
         } else {
           bufxp=bufxp+1;
         }
         buflen=buflen-keylen;
         key=key+keylen;
         xattrs++;
       }
    }
       /* ??????? i think this is bsd version of xattrs - needs to be posixizedr  */
    return xattrs;
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
  summary->minossint1=0;
  summary->maxossint1=0;
  summary->totossint1=0;
  summary->minossint2=0;
  summary->maxossint2=0;
  summary->totossint2=0;
  summary->minossint3=0;
  summary->maxossint3=0;
  summary->totossint3=0;
  summary->minossint4=0;
  summary->maxossint4=0;
  summary->totossint4=0;
  return 0;
}

int sumit (struct sum *summary,struct work *pwork) {

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
  if (pwork->ossint2 < summary->minossint2) summary->minossint1=pwork->ossint2;
  if (pwork->ossint2 > summary->maxossint2) summary->maxossint1=pwork->ossint2;
  summary->totossint2=summary->totossint2+pwork->ossint2;
  if (pwork->ossint3 < summary->minossint3) summary->minossint1=pwork->ossint3;
  if (pwork->ossint3 > summary->maxossint3) summary->maxossint1=pwork->ossint3;
  summary->totossint3=summary->totossint3+pwork->ossint3;
  if (pwork->ossint4 < summary->minossint4) summary->minossint1=pwork->ossint4;
  if (pwork->ossint4 > summary->maxossint4) summary->maxossint1=pwork->ossint4;
  summary->totossint4=summary->totossint4+pwork->ossint4;
  if (pwork->xattrs > 0) summary->totxattr++;
  return 0;
}

int tsumit (struct sum *sumin,struct sum *smout) {

  pthread_mutex_lock(&sum_mutex);

  smout->totsubdirs++;
  if (sumin->totfiles > smout->maxsubdirfiles) smout->maxsubdirfiles = sumin->totfiles;
  if (sumin->totlinks > smout->maxsubdirlinks) smout->maxsubdirlinks = sumin->totlinks;
  if (sumin->totsize  > smout->maxsubdirsize)  smout->maxsubdirsize  = sumin->totsize;

  smout->totfiles += sumin->totfiles;
  smout->totlinks += sumin->totlinks;
  smout->totsize  += sumin->totsize;

  /* only set these mins and maxes if there are files in the directory
     otherwise mins are all zero */
  if (sumin->totfiles > 0) {
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
int mkpath(char* file_path, mode_t mode) {
  char* p;
  char sp[MAXPATH];

  sprintf(sp,"%s", file_path);
  for (p=strchr(file_path+1, '/'); p; p=strchr(p+1, '/')) {
    //printf("mkpath mkdir file_path %s p %s\n", file_path,p);
    *p='\0';
    //printf("mkpath mkdir file_path %s\n", file_path);
    //if (mkdir(file_path, mode)==-1) {
    if (mkdir(file_path, mode | S_IRWXU)==-1) {
      if (errno!=EEXIST) {
         *p='/';
         return -1;
      }
    }
    *p='/';
  }
  //printf("mkpath mkdir sp %s\n",sp);
  mkdir(sp,mode);
  return 0;
}

int dupdir(struct work *pwork)
{
    char topath[MAXPATH];
    int rc;

    sprintf(topath,"%s/%s",in.nameto,pwork->name);
    //printf("mkdir %s\n",topath);
    // the writer must be able to create the index files into this directory so or in S_IWRITE
    //rc = mkdir(topath,pwork->statuso.st_mode | S_IWRITE);
    rc = mkdir(topath,pwork->statuso.st_mode | S_IRWXU);
    if (rc != 0) {
      //perror("mkdir");
      if (errno == ENOENT) {
        //printf("calling mkpath on %s\n",topath);
        mkpath(topath,pwork->statuso.st_mode);
      } else if (errno == EEXIST) {
        return 0;
      } else {
        return 1;
      }
    }
    chown(topath, pwork->statuso.st_uid,pwork->statuso.st_gid);
    // we dont need to set xattrs/time on the gufi directory those are in the db
    // the gufi directory structure is there only to walk, not to provide
    // information, the information is in the db

    return 0;
}

int incrthread() {
  pthread_mutex_lock(&running_mutex);
  runningthreads++;
  pthread_mutex_unlock(&running_mutex);
  return 0;
}

int decrthread() {
  pthread_mutex_lock(&running_mutex);
  runningthreads--;
  pthread_mutex_unlock(&running_mutex);
  return 0;
}

int getqent() {
  int mylqent;
  pthread_mutex_lock(&queue_mutex);
  mylqent=addrqent();
  pthread_mutex_unlock(&queue_mutex);
  return mylqent;
}

int pushdir( void  * qqwork) {

#if 0
  pthread_mutex_lock(&queue_mutex);
  pushn(qqwork);
  pthread_mutex_unlock(&queue_mutex);

#else
  // probably not much contention for the work-queue lock in most cases,
  // but this minimizes the time spent holding it.
  struct work* queued = pushn2_part1((struct work*)qqwork);

  pthread_mutex_lock(&queue_mutex);
  pushn2_part2(queued);
  pthread_mutex_unlock(&queue_mutex);
#endif

  return 0;
}

// get the index of this thread in the threadpool
int gettid() {
   return thpool_thread_index(mythpool, pthread_self());
}

int shortpath(const char *name, char *nameout, char *endname) {
     char prefix[MAXPATH];
     char *pp;
     int i;

     *endname = 0;              // in case there's no '/'
     i=0;
     sprintf(prefix,"%s",name);
     i=strlen(prefix);
     pp=prefix+i;
     //printf("cutting name down %s len %d\n",prefix,i);
     while (i > 0) {
       if (!strncmp(pp,"/",1)) {
          bzero(pp,1);
          sprintf(endname,"%s",pp+1);
          break;
       }
       pp--;
       i--;
     }
     sprintf(nameout,"%s",prefix);
     return 0;
}


// FKA "putils.c"

int processdirs(DirFunc dir_fn) {

     int myqent;
     struct work * workp;

     // loop over queue entries and running threads and do all work until
     // running threads zero and queue empty
     myqent=0;
     runningthreads=0;
     while (1) {
        myqent=getqent();
        if (runningthreads == 0) {
          if (myqent == 0) {
            break;
          }
        }
        if (runningthreads < in.maxthreads) {
          if (myqent > 0) {
            // we have to lock around all queue ops
            pthread_mutex_lock(&queue_mutex);
            workp=addrcurrents();
            incrthread();

            // this takes this entry off the queue but does NOT free the
            // buffer, that has to be done in dir_fn(), something like:
            // "free(((struct work*)workp)->freeme)"
            thpool_add_work(mythpool, dir_fn, (void *)workp);
            delQueuenofree();
            pthread_mutex_unlock(&queue_mutex);
          }
        }
     }

     return 0;
}

int printit(const char *name, const struct stat *status, char *type, char *linkname, int xattrs, char * xattr,int printing, long long pinode) {
  char buf[MAXXATTR];
  char bufv[MAXXATTR];
  char * xattrp;
  int cnt;
  if (!printing) return 0;
  if (!strncmp(type,"l",1)) printf("l ");
  if (!strncmp(type,"f",1)) printf("f ");
  if (!strncmp(type,"d",1)) printf("d ");
  printf("%s ", name);
  if (!strncmp(type,"l",1)) printf("-> %s ",linkname);
  printf("%lld ", status->st_ino);
  printf("%lld ", pinode);
  printf("%d ",status->st_mode);
  printf("%d ",status->st_nlink);
  printf("%d ", status->st_uid);
  printf("%d ", status->st_gid);
  printf("%lld ", status->st_size);
  printf("%d ", status->st_blksize);
  printf("%lld ", status->st_blocks);
  printf("%ld ", status->st_atime);
  printf("%ld ", status->st_mtime);
  printf("%ld ", status->st_ctime);
  cnt = 0;
  xattrp=xattr;
  if (xattrs > 0) {
    printf("xattr: %s",xattr);
/*
    while (cnt < xattrs) {
      bzero(buf,sizeof(buf));
      bzero(bufv,sizeof(bufv));
      strcpy(buf,xattrp); xattrp=xattrp+strlen(buf)+1;
      printf("%s",buf);
      strcpy(bufv,xattrp); xattrp=xattrp+strlen(bufv)+1;
      printf("%s ",bufv);
      cnt++;
   }
*/
  }
  printf("\n");
  return 0;
}

/* the following is for the triell */

/* Adapted to character representation of long long from                  */
/* http://www.techiedelight.com/trie-implementation-insert-search-delete/ */
#include <stdio.h>
#include <stdlib.h>

// Function that returns a new Trie node
struct Trie* getNewTrieNode()
{
    struct Trie* node = (struct Trie*)malloc(sizeof(struct Trie));
    node->isLeaf = 0;

    for (int i = 0; i < CHAR_SIZE; i++)
        node->character[i] = NULL;

    return node;
}

// Iterative function to insert a string in Trie.
void insertll(struct Trie* *head, char* str)
{
    // start from root node
    struct Trie* curr = *head;
    while (*str)
    {
        // create a new node if path doesn't exists
        if (curr->character[*str - '0'] == NULL)
            curr->character[*str - '0'] = getNewTrieNode();

        // go to next node
        curr = curr->character[*str - '0'];

        // move to next character
        str++;
    }

    // mark current node as leaf
    curr->isLeaf = 1;
}

// Iterative function to search a string in Trie. It returns 1
// if the string is found in the Trie, else it returns 0
int searchll(struct Trie* head, char* str)
{
    // return 0 if Trie is empty
    if (head == NULL)
        return 0;

    struct Trie* curr = head;
    while (*str)
    {
        // go to next node
        curr = curr->character[*str - '0'];

        // if string is invalid (reached end of path in Trie)
        if (curr == NULL)
            return 0;

        // move to next character
        str++;
    }

    // if current node is a leaf and we have reached the
    // end of the string, return 1
    return curr->isLeaf;
}

// returns 1 if given node has any children
int haveChildren(struct Trie* curr)
{
    for (int i = 0; i < CHAR_SIZE; i++)
        if (curr->character[i])
            return 1;    // child found

    return 0;
}

// Recursive function to delete a string in Trie.
int deletionll(struct Trie* *curr, char* str)
{
    // return if Trie is empty
    if (*curr == NULL)
        return 0;

    // if we have not reached the end of the string
    if (*str)
    {
        // recurse for the node corresponding to next character in
        // the string and if it returns 1, delete current node
        // (if it is non-leaf)
        if (*curr != NULL && (*curr)->character[*str - '0'] != NULL &&
            deletionll(&((*curr)->character[*str - '0']), str + 1) &&
            (*curr)->isLeaf == 0)
        {
            if (!haveChildren(*curr))
            {
                free(*curr);
                (*curr) = NULL;
                return 1;
            }
            else {
                return 0;
            }
        }
    }

    // if we have reached the end of the string
    if (*str == '\0' && (*curr)->isLeaf)
    {
        // if current node is a leaf node and don't have any children
        if (!haveChildren(*curr))
        {
            free(*curr); // delete current node
            (*curr) = NULL;
            return 1; // delete non-leaf parent nodes
        }

        // if current node is a leaf node and have children
        else
        {
            // mark current node as non-leaf node (DON'T DELETE IT)
            (*curr)->isLeaf = 0;
            return 0;       // don't delete its parent nodes
        }
    }

    return 0;
}

/* end of  the triell */
