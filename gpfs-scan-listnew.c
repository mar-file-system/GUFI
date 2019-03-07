#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <gpfs.h>

extern int errno;

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
