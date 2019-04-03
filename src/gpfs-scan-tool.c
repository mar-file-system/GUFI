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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <pthread.h>
#include <ctype.h>
#include <gpfs.h>
#include "bf.h"

pthread_mutex_t lock;
int gotit;
#define MPATH 1024

struct passvars {
   char passoutfile[MPATH];
   int numthreads;
   int threadnum;
   long long intime;
   long long stride;
   gpfs_fssnap_handle_t *fsp;
};


char *format_xattrs(char returnStr[], gpfs_iscan_t *iscanP,
                         const char *xattrP, unsigned int xattrLen)
 {
   int rc;
   int i;
   const char *nameP;
   const char *valueP;
   char *xattrStr = (char *)malloc(xattrLen);
   unsigned int valueLen;
   const char *xattrBufP = xattrP;
   unsigned int xattrBufLen = xattrLen;
   int printable;
   returnStr = (char *)malloc(xattrLen);

   while ((xattrBufP != NULL) && (xattrBufLen > 0)) {
     rc = gpfs_next_xattr(iscanP, &xattrBufP, &xattrBufLen,
                          &nameP, &valueLen, &valueP);
     if (rc != 0) {
       rc = errno;
       fprintf(stderr, "gpfs_next_xattr: %s\n", strerror(rc));
       break;
     }
 
     if (nameP == NULL) {
       continue;
     }
    
      //We don't need user.marfs xattrs
     if (strstr(nameP, "user.marfs") != NULL) {
       continue;
     }
     
     snprintf(xattrStr, xattrLen, " \"%s\"=", nameP);
      
      if (valueLen == 0) {
       printable = 0;
       }  else {
         printable = 1;
         for (i = 0; i < (valueLen-1); i++)
           if (!isprint(valueP[i]))
             printable = 0;
         if (printable)
         {
           if (valueP[valueLen-1] == '\0')
             valueLen -= 1;
           else if (!isprint(valueP[valueLen-1]))
             printable = 0;
         }
       }
         if (printable) 
         strcat(xattrStr, valueP);
         else 
           continue;  
     
     strcat(returnStr, xattrStr); 
  }
  free(xattrStr);
  xattrStr = NULL;
  return returnStr;
}

/* Scan inodes in the gpfs mount point */
void *proc_inodes (void *args) {
  struct passvars *pargs = (struct passvars*)args;
  int rc = 0;
  const gpfs_iattr64_t *iattrp;
  gpfs_fssnap_handle_t *fsp = NULL;
  gpfs_iscan_t *iscanp = NULL;
  long long int intimell;
  char type[10];
  int printit;
  unsigned long long int maxinode;
  unsigned long long int seekp;
  FILE* outfile;
  char outfilename[MPATH];
  int mythread;
  int totthreads;
  long long stride;
  int bigmoves;
  char *delim;
  int nxattrs; 
  const char **xattrNames = NULL;
  const char *xattrBuf = NULL;
  unsigned int xattrBufLen;
  char *returnStr = NULL;
  char linkBuf[4096];
 
  mythread=pargs->threadnum;
  totthreads=pargs->numthreads;
  intimell=pargs->intime;
  stride=pargs->stride;
  delim=in.delim;
  // We always want to return all xattrs, so we set nxattrs to -1 
  // which will make gpfs_open_inodescan_with_xatttrs64() return all xattrs 
  nxattrs=-1;

  sprintf(outfilename,"%s.%d",pargs->passoutfile,mythread);
  fsp=pargs->fsp;
  /* copied all the variables, release the lock */
  gotit=1;

  outfile = fopen(outfilename, "w");
  // should check return for NULL

  // Open an inode scan on the file system
  iscanp = gpfs_open_inodescan_with_xattrs64(fsp, NULL, nxattrs, xattrNames, &maxinode);
  if (iscanp == NULL) {
    rc = errno;
    fprintf(stderr, "gpfs_open_inodescan error %s\n", strerror(rc));
    goto scanexit;
  }

  seekp=stride*mythread;
  if (seekp <= maxinode) {
    gpfs_seek_inode64(iscanp,seekp);
  } else {
    goto scanexit;
  }

  bigmoves = 0;

  while (1) {

    rc = gpfs_next_inode_with_xattrs64(iscanp, 0, &iattrp, &xattrBuf, &xattrBufLen);
    if (rc != 0) {
      int saveerrno = errno;
      fprintf(stderr, "gpfs_next_inode rc %d error %s\n", rc, strerror(saveerrno));
      rc = saveerrno;
      goto scanexit;
    }

    // check to see if we are done with this batch
    // check to see if we are done with all batches
    if (iattrp == NULL) break;
    if (iattrp->ia_inode > (seekp + stride)) {
      bigmoves++;
      seekp=(bigmoves*totthreads*stride) + (mythread*stride);
      if (seekp <= maxinode) {
         gpfs_seek_inode64(iscanp,seekp);
         continue;
       } else {
         goto scanexit;
       }
    }

    bzero(type,sizeof(type));
    printit=0;
 
  // This conditional is for incremental scans, and will receive an epoch time value from the file
  // .lastrun  
  if ((iattrp->ia_mtime.tv_sec >= intimell) || (iattrp->ia_ctime.tv_sec >= intimell)) {

      if (S_ISDIR(iattrp->ia_mode)) {
        printit=1;
        sprintf(type,"d");
      }

      if (S_ISLNK(iattrp->ia_mode)) {
        if (iattrp->ia_createtime.tv_sec <= intimell) printit=1;
          sprintf(type,"l");
      }

      if (S_ISREG(iattrp->ia_mode)) {
        if (iattrp->ia_createtime.tv_sec <= intimell)  printit=1;
          sprintf(type,"f");
      }
  }
 
  // This conditional is for loading a full
  if (intimell == 0) {
      fprintf(outfile,"%llu%s",iattrp->ia_inode,delim);
      fprintf(outfile,"%s%s",type,delim);
      fprintf(outfile,"%u%s",iattrp->ia_mode,delim);
      fprintf(outfile,"%lld%s",iattrp->ia_nlink,delim);
      fprintf(outfile,"%llu%s",iattrp->ia_uid,delim);
      fprintf(outfile,"%llu%s",iattrp->ia_gid,delim);
      fprintf(outfile,"%lld%s",iattrp->ia_size,delim);
      fprintf(outfile,"%d%s",iattrp->ia_blocksize,delim);
      fprintf(outfile,"%lld%s",iattrp->ia_blocks,delim);
      fprintf(outfile,"%lld%s",iattrp->ia_atime.tv_sec,delim);
      fprintf(outfile,"%lld%s",iattrp->ia_mtime.tv_sec,delim);
      fprintf(outfile,"%lld%s",iattrp->ia_ctime.tv_sec,delim);

      // This returns the relative link path
      if (S_ISLNK(iattrp->ia_mode)) {
        rc = gpfs_ireadlink64(fsp, iattrp->ia_inode, linkBuf, sizeof(linkBuf));
        if (rc == -1) {
          int saveerrno = errno;
          fprintf(stderr, "gpfs_readlink %d error %s\n", rc, strerror(saveerrno));
          rc = saveerrno;
          goto scanexit;
        }
        fprintf(outfile,"%s%s", linkBuf, delim);
        fprintf(outfile,"%s\n", delim);
        continue;
      } else {
        fprintf(outfile,"%s", delim);
      }
     
      if (xattrBufLen > 0) {
        returnStr = format_xattrs(returnStr,iscanp, xattrBuf, xattrBufLen);
        fprintf(outfile,"%s%s",returnStr,delim);
        free(returnStr);
        returnStr = NULL;
      } else {
        fprintf(outfile,"%s", delim);
      }
      fprintf(outfile,"%lld%s\n",iattrp->ia_createtime.tv_sec,delim);
    } 
    
  if (printit == 1 && intimell > 0) {
        fprintf(outfile,"%llu %s\n",iattrp->ia_inode,type);
  }

}
scanexit:
  fclose(outfile);

  if (iscanp) {
    gpfs_close_inodescan(iscanp);
  }
  return 0;
}

int validate_inputs(unsigned long long int intime) {

  printf("threadnum: %d; stride: %d; delimiter: %s;\n", in.maxthreads, in.stride, in.delim);
  printf("scanning under %s\n", in.name);
  if (intime == 0) {
    printf(".lastrun doesn't exist, running a full\n");
  } else {
    printf(".lastrun exists, running incremental\n");
  }
  return 0;
}

void sub_help() {
   printf("input_dir         top of the file tree to scan\n ");
   printf("\n");
   printf("The GPFS Scanning tool will scan a GPFS mount point and create a file per thread that can be parsed\n");
   printf("by ohther GUFI tools(like bfwi). The file will contain file paths, inode, and file attrs and xattrs.\n");
   printf("This scanner can scan a tree to do a full GUFI load, or scan for incremental changes. If the file .lastrun,\n");
   printf("containing an epoch timestamp, exists in the directory you start the scan from(we should make this more configurable later),\n");
   printf("then an incremental scan will be run. If not, a full scan will be run.\n");
   printf("\n");
   printf("Defaults: Threads: 1; Stride: 0; Delimiter: |");
   printf("\n");
}

/* main */
int main(int argc, char *argv[]) {
  FILE *lastrun;

  int i;
  int rc = NULL;
  char pastsec[11];
  char outfilename[1024];
  time_t now = time(NULL);
  unsigned long long int intime;
  struct tm* timenow;
  gpfs_fssnap_handle_t *fsp = NULL;
  pthread_t workers[1024];
  struct passvars *args;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
  int idx = parse_cmd_line(argc, argv, "hHn:d:g:", 1, "input_dir", &in);
  lastrun = fopen(".lastrun", "r");
  if (lastrun == NULL) {
    intime=atoll(pastsec);
  } else {
    lastrun = fopen(".lastrun", "r");
    fgets(pastsec,11,lastrun);
    fclose(lastrun);
    intime=atoll(pastsec);
  }

  if (in.helped)
    sub_help();
  if (idx < 0)
    return -1;
  else {
    // parse positional args, following the options
    int retval = 0;
    INSTALL_STR(in.name,   argv[idx++], MAXPATH, "input_dir");

    if (retval)
      return retval;
  }
    if (validate_inputs(intime))
      return -1;
  
  timenow = localtime(&now);
  strftime(outfilename,sizeof(outfilename),"scan-results-%Y-%m-%d_%H:%M",timenow);

  /* Open the file system mount */
  fsp = gpfs_get_fssnaphandle_by_path(in.name);
  if (fsp == NULL) {
    rc = errno;
    fprintf(stderr, "gpfs_get_fssnaphandle_by_path for %s error %s\n", in.name, strerror(rc));
    exit(-1);
  }

  /* load the non changing parts of the struct to pass to the threads */
  args=malloc(sizeof(struct passvars));
  sprintf(args->passoutfile,"%s",outfilename);;
  args->numthreads=in.maxthreads;
  args->fsp=fsp;
  args->intime=intime;
  args->stride=in.stride;
  
  if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init has failed\n");
        return 1;
    }

  i=0;
  while (i < in.maxthreads) {
    gotit=0;
    pthread_mutex_lock(&lock);
    args->threadnum=i;
    printf("starting threadnum %d\n",args->threadnum);
    if(pthread_create(&workers[i], NULL, proc_inodes, args)) {
      free(args);
      gpfs_free_fssnaphandle(fsp);
      fprintf(stderr, "error starting threads\n");
      exit(-1);
    }
    while (gotit==0) {
    }
    pthread_mutex_unlock(&lock);
    i++;
  }

     /* Telling the main thread to wait for the task completion of all its spawned threads.*/
  for (i = 0; i < in.maxthreads; i++) {
    pthread_join (workers[i], NULL);
  }


  free(args);
  pthread_mutex_destroy(&lock);
  lastrun = fopen(".lastrun", "w");
  fprintf(lastrun,"%lu",now);
  gpfs_free_fssnaphandle(fsp);
  fclose(lastrun);
  return rc;
}
