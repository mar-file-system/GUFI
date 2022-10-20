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


#define _GNU_SOURCE
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>

#include "bf.h"
#include "utils.c"
#include "xattrs.h"

extern int errno;

static int printit(const char *name, const struct stat *status, char *type, char *linkname, struct xattrs *xattrs, int printing, long long pinode) {
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

static int printload(const char *name, const struct stat *status, char *type, char *linkname, struct xattrs *xattrs, long long pinode, char *sortf, FILE *of) {
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

void listdir(const char *name, long long int level, struct dirent *entry, long long int pin, int statit, int xattrit,int loader )
{
    DIR *dir;
    //struct dirent *entry;
    char path[MAXPATH];
    char lpath[MAXPATH];
    struct stat st;
    char type[2];
    struct xattrs xattrs;
    char sortf[MAXPATH];
    char beginpath[MAXPATH];
    char endpath[MAXPATH];

    //printf("inlistdir name %s\n",name);
    if (statit) {
      lstat(name,&st);
      if (in.dontdescend == 0) {
        if (S_ISDIR(st.st_mode)) {
           dir = opendir(name);
        }
      }
    } else {
      if (in.dontdescend == 0) {
        if (!(dir = opendir(name)))
          return;
      }
    }
    //if (statit) lstat(name,&st);
    bzero(&xattrs, sizeof(xattrs));
    if (xattrit) {
      xattrs_get(name, &xattrs);
    }
    if (statit+xattrit > 0) {
      bzero(lpath,sizeof(lpath));
      bzero(type,sizeof(type));
      //printf("%lld d ", level);
      SNPRINTF(type,2,"f");
      if (S_ISDIR(st.st_mode)) {
        SNPRINTF(type,2,"d");
      }
      if (S_ISLNK(st.st_mode)) {
        SNPRINTF(type,2,"l");
      }
      if (loader>0) {
        if (S_ISDIR(st.st_mode)) {
          SNPRINTF(sortf,MAXPATH,"%s%s%s",name,in.delim,type);
        } else {
          shortpath(name,beginpath,endpath);
          SNPRINTF(sortf,MAXPATH,"%s%s%s",beginpath,in.delim,type);
        }
        printload(name,&st,type,lpath,&xattrs,pin,sortf,stdout);
      } else {
        printit(name,&st,type,lpath,&xattrs,1,pin);
      }
    } else {
      printf("d %s %lld %lld\n", name, pin , level);
    }

    if (statit) {
      if (!S_ISDIR(st.st_mode)) {
         return;
      }
      if (in.dontdescend==1) {
         return;
      }
    }

    while ((entry = (readdir(dir)))) {
       int len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
       path[len] = 0;
       if (statit) {
         lstat(path,&st);
       } else {
         st.st_mode=st.st_mode|S_IRUSR|S_IWUSR|S_IXUSR;
         if (entry->d_type == DT_DIR) {
           st.st_mode=040755;
         } else if (entry->d_type == DT_LNK) {
           st.st_mode=0120755;
         } else {
           st.st_mode=0100644;
         }
       }
       bzero(&xattrs, sizeof(xattrs));
       if (xattrit) {
         xattrs_get(path, &xattrs);
       }
        //if (entry->d_type == DT_DIR) {
        if (S_ISDIR(st.st_mode) ) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            SNPRINTF(type,2,"d");
            //printf("inwhile d %s %lld %lld\n", name, entry->d_ino, ppin);
            listdir(path, pin, entry, entry->d_ino, statit, xattrit,loader);
        } else {
            len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            bzero(lpath,sizeof(lpath));
            //if (entry->d_type == DT_REG) {
            if (S_ISREG(st.st_mode)) {
               SNPRINTF(type,2,"f");
            }
            if (S_ISLNK(st.st_mode)) {
               SNPRINTF(type,2,"l");
               if (statit) readlink(path,lpath,MAXPATH);
            }
            if (statit+xattrit > 0) {
              //printf("%lld ", pin);
              //bzero(type,sizeof(type));
              //printf("readlink %s %s\n",path,lpath);
              if (loader>0) {
                if (S_ISDIR(st.st_mode)) {
                  SNPRINTF(sortf,MAXPATH,"%s%s%s",path,in.delim,type);
                } else {
                  shortpath(path,beginpath,endpath);
                  SNPRINTF(sortf,MAXPATH,"%s%s%s",beginpath,in.delim,type);
                }
                printload(path,&st,type,lpath,&xattrs,pin,sortf,stdout);
              } else {
                printit(path,&st,type,lpath,&xattrs,1,pin);
              }
            } else {
              printf("%s %s %"STAT_ino" %lld\n",type, path, entry->d_ino,pin);
            }
        }
    }
    closedir(dir);
}

void helpme() {
    fprintf (stderr, "dfw depth first file system tree walk printing:\n");
    fprintf (stderr, "path, type, stat info, linkname\n");
    fprintf (stderr, "optionally if asked for xattrs \n");
    fprintf (stderr, "syntax dfw -s -x -h -d -l directorytowalk\n");
    fprintf (stderr, "-s stat everything - otherwise this is just a readdirplus walk \n");
    fprintf (stderr, "-x get xattrs \n");
    fprintf (stderr, "-d delimiter (one char)  [use 'x' for 0x%02X]\n", (uint8_t)fielddelim[0]);
    fprintf (stderr, "-D dont traverse/descend the tree, just stat this directory or file\n");
    fprintf (stderr, "-l print in loader format (only works if -x and -s are provided \n");
    fprintf (stderr, "-h help \n");
}

int main(int argc, char **argv)
{
    struct dirent *entries = NULL;
    struct stat status;
    int statit;
    int xattrit;
    int c;
    int numops;
    int loader;

    statit=0;
    xattrit=0;
    numops=0;
    loader=0;
    in.dontdescend=0;
    while ((c = getopt (argc, argv, "sxlhDd:")) != -1)
      switch (c)
        {
        case 's':
          statit=1;
          numops++;
          break;
        case 'x':
          xattrit=1;
          numops++;
          break;
        case 'l':
          loader=1;
          numops++;
          break;
        case 'd':
          if (optarg[0] == 'x') {
            in.delim[0] = fielddelim[0];
          }
          else {
            in.delim[0] = optarg[0];
          }
          break;
        case 'D':
          in.dontdescend=1;
          numops++;
          break;
        case 'h':
          helpme();
          exit(0);
        //default:
      }
    if (argc <= optind) {
      helpme();
      exit(-1);
    }
    if ((in.dontdescend==1) && (statit<1)) {
      fprintf(stderr,"-D must also have -s\n");
      exit(-1);
    }
    //printf("argc=%d x=%d s=%d l=%d dod=%d d=%s non opt arg %s optind %d\n",argc,xattrit,statit,loader,in.dodelim,in.delim,argv[optind],optind);

    lstat(argv[optind],&status);
    //printf("inmain d %s %lld 0\n", argv[1],status.st_ino);
    listdir(argv[optind], 0, entries, status.st_ino,statit,xattrit,loader);
    return 0;
}
