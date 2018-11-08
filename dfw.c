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



#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <ctype.h>
#include <errno.h>
#include <utime.h> 

#include "bf.h"
#include "utils.c"
 
void listdir(const char *name, long long int level, struct dirent *entry, long long int pin, int statit, int xattrit )
{
    DIR *dir;
    //struct dirent *entry;
    char path[1024];
    char lpath[1024];
    long long int ppin;
    struct stat st;
    char type[2];
    int xattrs;
    char xattr[MAXXATTR];

    //printf("inlistdir name %s\n",name);
    if (!(dir = opendir(name)))
        return;
    if (statit) lstat(name,&st);
    bzero(xattr, sizeof(xattr));
    xattrs=0;
    if (xattrit) {
      xattrs=pullxattrs(name,xattr);
    }
    if (statit+xattrit > 0) {
      bzero(lpath,sizeof(lpath));
      bzero(type,sizeof(type));
      //printf("%lld d ", level);
      sprintf(type,"d");
      printit(name,&st,type,lpath,xattrs,xattr,1,pin);
    } else {
      printf("d %s %lld %lld\n", name, pin , level);
    }
    ppin=pin;
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
       xattrs=0;
       bzero(xattr, sizeof(xattr));
       if (xattrit) {
         xattrs=pullxattrs(path,xattr);
       }
        //if (entry->d_type == DT_DIR) {
        if (S_ISDIR(st.st_mode) ) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
                continue;
            sprintf(type,"d");
            //printf("inwhile d %s %lld %lld\n", name, entry->d_ino, ppin);
            listdir(path, pin, entry, entry->d_ino, statit, xattrit);
        } else {
            len = snprintf(path, sizeof(path)-1, "%s/%s", name, entry->d_name);
            bzero(lpath,sizeof(lpath));
            //if (entry->d_type == DT_REG) {
            if (S_ISREG(st.st_mode)) {
               sprintf(type,"f");
            }
            if (S_ISLNK(st.st_mode)) {
               sprintf(type,"l");
               if (statit) readlink(path,lpath,MAXPATH);
            }
            if (statit+xattrit > 0) {
              //printf("%lld ", pin);
              //bzero(type,sizeof(type));
              //printf("readlink %s %s\n",path,lpath);
              printit(path,&st,type,lpath,xattrs,xattr,1,pin);
            } else {
              printf("%s %s %lld %lld\n",type, path, entry->d_ino,pin);
            }
        }
    }
    closedir(dir);
}
 
int main(int argc, char *argv[])
{
    struct dirent *entries;
    struct stat status;
    int statit;
    int xattrit;
    statit=atoi(argv[2]);
    xattrit=atoi(argv[3]);
    lstat(argv[1],&status);
    //printf("inmain d %s %lld 0\n", argv[1],status.st_ino);
    listdir(argv[1], 0, entries, status.st_ino,statit,xattrit);
    return 0;
}
 
