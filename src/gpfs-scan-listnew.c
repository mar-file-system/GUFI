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

#include <gpfs.h>

/* Scan inodes in the gpfs mount point */
int proc_inodes(char *pathp,char *intime)
{
  int rc = 0;
  const gpfs_iattr_t *iattrp;
  gpfs_fssnap_handle_t *fsp = NULL;
  gpfs_iscan_t *iscanp = NULL;
  int64 intimell;
  int recordit;
  char type[10];

  intimell=atoll(intime);

  /* Open the file system mount */
  fsp = gpfs_get_fssnaphandle_by_path(pathp);
  if (fsp == NULL) {
    rc = errno;
    fprintf(stderr, "gpfs_get_fssnaphandle_by_path for %s error %s\n", pathp, strerror(rc));
    goto scanexit;
  }

  /* Open an inode scan on the file system */
  iscanp = gpfs_open_inodescan(fsp, NULL, NULL);
  if (iscanp == NULL) {
    rc = errno;
    fprintf(stderr, "gpfs_open_inodescan for %s error %s\n", pathp, strerror(rc));
    goto scanexit;
  }

/* Loop through inodes  and make a list of all the inodes that have a date larger than in date*/
  while (1) {

    rc = gpfs_next_inode(iscanp, 0, &iattrp);
    if (rc != 0) {
      int saveerrno = errno;
      fprintf(stderr, "gpfs_next_inode for %s rc %d error %s\n",pathp, rc, strerror(saveerrno));
      rc = saveerrno;
      goto scanexit;
    }

    /* finished? */
    if (iattrp == NULL) break;

    bzero(type,sizeof(type));
    recordit=0;
    if ((iattrp->ia_mtime.tv_sec >= intimell) || (iattrp->ia_ctime.tv_sec >= intimell)) {
      if (S_IFDIR(iattrp->ia_mode)){
         recordit++;
         sprintf(type,"d");
      }
      if (S_IFLNK(iattrp->ia_mode)){
         recordit++;
         sprintf(type,"l");
      }
      if (S_IFREG(iattrp->ia_mode)){
         recordit++;
         sprintf(type,"f");
      }

      printf("%d %s",iattrp->ia_inode,type);

    }

  }

scanexit:
  if (iscanp)
    gpfs_close_inodescan(iscanp);
  if (fsp)
    gpfs_free_fssnaphandle(fsp);

  return rc;
}

/* main */
int main(int argc, char *argv[])
{
  int rc;

  if (argc != 3) {
    fprintf(stderr,"just provide the mount point and the date info\n");
    exit(-1);
  }

  /* process the inodes */
  rc = proc_inodes(argv[1],argv[2]);
  return rc;
}
