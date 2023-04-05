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

#include <gpfs.h>
//for testing where gpfs doesnt exisst
//#include <mygpfs.h>

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

/* Scan inodes in the gpfs mount point */
void *proc_inodes (void *args) {
  struct passvars *pargs = (struct passvars*)args;
  int rc = 0;
  const gpfs_iattr64_t *iattrp;
  gpfs_fssnap_handle_t *fsp = NULL;
  gpfs_iscan_t *iscanp = NULL;
  long long int intimell;
  int recordit;
  char type[10];
  long long int gpfsnum;
  long long int gpfsrec;
  int printit;
  unsigned long long int maxinode;
  unsigned long long int seekp;
  FILE* outfile;
  char outfilename[MPATH];
  int mythread;
  int totthreads;
  long long stride;
  int bigmoves;

  //sleep(pargs->threadnum+1);
  mythread=pargs->threadnum;
  totthreads=pargs->numthreads;
  intimell=pargs->intime;
  stride=pargs->stride;
  sprintf(outfilename,"%s.%d",pargs->passoutfile,mythread);
  fsp=pargs->fsp;
  /* copied all the variables, release the lock */
  gotit=1;

  // to test out the thread launch and pass
  //printf("thread %d working tot threads %d passoutfile %s intime %llu stride %lld \n",mythread,totthreads,outfilename,intimell,stride);
  //return 0;

  outfile = fopen(outfilename, "w");
  // should check return for NULL

  // Open an inode scan on the file system
  iscanp = gpfs_open_inodescan64(pargs->fsp, NULL, &maxinode);
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


  bigmoves = 1;
  gpfsnum = 0;
  gpfsrec = 0;

  while (1) {

    rc = gpfs_next_inode64(iscanp, 0, &iattrp);
    if (rc != 0) {
      int saveerrno = errno;
      fprintf(stderr, "gpfs_next_inode rc %d error %s\n", rc, strerror(saveerrno));
      rc = saveerrno;
      goto scanexit;
    }

    // check to see if we are done with this batch
    // check to see if we are done with all batches
    if (iattrp == NULL) break;
    if (iattrp->ia_inode > ((bigmoves*totthreads*stride)+((mythread+1)*stride) ) ) {
      bigmoves++;
      seekp=(bigmoves+totthreads*stride) + (mythread*stride);
      if (seekp <= maxinode) {
        gpfs_seek_inode64(iscanp,seekp);
        continue;
      } else {
        goto scanexit;
      }
    }

    bzero(type,sizeof(type));
    recordit=0;
    printit=0;
    if ((iattrp->ia_mtime.tv_sec >= intimell) || (iattrp->ia_ctime.tv_sec >= intimell)) {
      if (S_ISDIR(iattrp->ia_mode)){
         printit=1;
         recordit++;
         sprintf(type,"d");
      }
      if (S_ISLNK(iattrp->ia_mode)){
         if (iattrp->ia_createtime.tv_sec <= intimell) printit=1;
         recordit++;
         sprintf(type,"l");
      }
      if (S_ISREG(iattrp->ia_mode)){
         if (iattrp->ia_createtime.tv_sec <= intimell) printit=1;
         recordit++;
         sprintf(type,"f");
      }
      gpfsnum++;
      if (printit==1) fprintf(outfile, "%llu %s\n",iattrp->ia_inode,type);
    }
    gpfsrec++;
  }

scanexit:
  /*printf("total %ld record %ld maxinode %ld\n",gpfsnum,gpfsrec,maxinode); */
  fclose(outfile);

  if (iscanp) {
    gpfs_close_inodescan(iscanp);
  }
  return 0;
}

/* main */
int main(int argc, char *argv[]) {
  FILE *lastrun;

  int i;
  int rc;
  char pastsec[11];
  char pathname[1024];
  char outfilename[1024];
  int numthreads;
  time_t now = time(NULL);
  long long int intime;
  long long int stride;
  struct tm* timenow;
  gpfs_fssnap_handle_t *fsp = NULL;
  pthread_t workers[1024];
  struct passvars *args;
  if (argc != 4) {
    fprintf(stderr,"just provide the mount point and numthreads and stride\n");
    exit(-1);
  }
  sprintf(pathname,"%s",argv[1]);
  numthreads=atol(argv[2]);
  stride=atoll(argv[3]);

  timenow = localtime(&now);
  strftime(outfilename,sizeof(outfilename),"scan-results-%Y-%m-%d_%H:%M",timenow);

  lastrun = fopen(".lastrun", "r");

  fgets(pastsec,11,lastrun);
  fclose(lastrun);
  intime=atoll(pastsec);

  /* Open the file system mount */
  fsp = gpfs_get_fssnaphandle_by_path(pathname);
  if (fsp == NULL) {
    rc = errno;
    fprintf(stderr, "gpfs_get_fssnaphandle_by_path for %s error %s\n", pathname, strerror(rc));
    exit(-1);
  }

  /* load the non changing parts of the struct to pass to the threads */
  args=malloc(sizeof(struct passvars));
  sprintf(args->passoutfile,"%s",outfilename);;
  args->numthreads=numthreads;
  args->fsp=fsp;
  args->intime=intime;
  args->stride=stride;

  if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init has failed\n");
        return 1;
    }

  i=0;
  while (i < numthreads) {
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
       //printf(".");
    }
    pthread_mutex_unlock(&lock);
    i++;
  }

     /* Telling the main thread to wait for the task completion of all its spawned threads.*/
  for (i = 0; i < numthreads; i++) {
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
