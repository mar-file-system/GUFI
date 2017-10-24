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

int printitd(const char *name, const struct stat *status, char *type, char *linkname, int xattrs, char * xattr,int printing, long long pinode) {
  char buf[MAXXATTR];
  char bufv[MAXXATTR];
  char * xattrp;
  int cnt;
  if (!printing) return 0;
  printf("%s%s",name,fielddelim);
  if (!strncmp(type,"l",1)) printf("l%s",fielddelim);
  if (!strncmp(type,"f",1)) printf("f%s",fielddelim);
  if (!strncmp(type,"d",1)) printf("d%s",fielddelim);
  printf("%lld%s", status->st_ino,fielddelim);
  printf("%lld%s", pinode,fielddelim);
  printf("%d%s",status->st_mode,fielddelim);
  printf("%d%s",status->st_nlink,fielddelim);
  printf("%d%s", status->st_uid,fielddelim);
  printf("%d%s", status->st_gid,fielddelim);
  printf("%lld%s", status->st_size,fielddelim);
  printf("%d%s", status->st_blksize,fielddelim);
  printf("%lld%s", status->st_blocks,fielddelim);
  printf("%ld%s", status->st_atime,fielddelim);
  printf("%ld%s", status->st_mtime,fielddelim);
  printf("%ld%s", status->st_ctime,fielddelim);
  if (!strncmp(type,"l",1)) printf("%s",linkname);
  printf("%s",fielddelim);
  cnt = 0;
  xattrp=xattr;
  if (xattrs > 0) {
    //printf("xattr: ");
    printf("%s",xattr);
/*
    while (cnt < xattrs) {
      bzero(buf,sizeof(buf));
      bzero(bufv,sizeof(bufv));
      strcpy(buf,xattrp); xattrp=xattrp+strlen(buf)+1;
      printf("%s%s",buf,xattrdelim); 
      strcpy(bufv,xattrp); xattrp=xattrp+strlen(bufv)+1; 
      printf("%s%s",bufv,xattrdelim); 
      cnt++;
    }
*/
  }
  printf("%s\n",fielddelim);
  return 0;
}



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
  summary->minuid=0;
  summary->maxuid=0;
  summary->mingid=0;
  summary->maxgid=0;
  summary->minsize=0;
  summary->maxsize=0;
  summary->totltk=0;
  summary->totmtk=0;
  summary->totltm=0;
  summary->totmtm=0;
  summary->totmtg=0;
  summary->totmtt=0;
  summary->totsize=0;
  summary->minctime=0;
  summary->maxctime=0;
  summary->minmtime=0;
  summary->maxmtime=0;
  summary->minatime=0;
  summary->maxatime=0;
  summary->minblocks=0;
  summary->maxblocks=0;
  summary->setit=0;
  summary->totxattr=0;
  summary->totsubdirs=0;
  summary->maxsubdirfiles=0;
  summary->maxsubdirlinks=0;
  summary->maxsubdirsize=0;
  return 0;
}

int sumit (struct sum *summary,struct stat *status, int xattrs, char * type) {

  if (summary->setit == 0) {
    summary->minuid    = status->st_uid;
    summary->maxuid    = status->st_uid;
    summary->mingid    = status->st_gid;
    summary->maxgid    = status->st_gid;
    summary->minsize   = status->st_size;
    summary->maxsize   = status->st_size;;
    summary->minctime  = status->st_ctime;
    summary->maxctime  = status->st_ctime;
    summary->minmtime  = status->st_mtime;
    summary->maxmtime  = status->st_mtime;
    summary->minatime  = status->st_atime;
    summary->maxatime  = status->st_atime;;
    summary->minblocks = status->st_blocks;
    summary->maxblocks = status->st_blocks;
    summary->setit     = 1;
  }
  if (!strncmp(type,"f",1)) {
     summary->totfiles++;
     if (status->st_size < summary->minsize) summary->minsize=status->st_size;
     if (status->st_size > summary->maxsize) summary->maxsize=status->st_size;
     if (status->st_size <= 1024) summary->totltk++;
     if (status->st_size > 1024) summary->totmtk++;
     if (status->st_size <= 1048576) summary->totltm++;
     if (status->st_size > 1048576) summary->totmtm++;
     if (status->st_size > 1073741824) summary->totmtg++;
     if (status->st_size > 1099511627776) summary->totmtt++;
     summary->totsize=summary->totsize+status->st_size;
     if (status->st_blocks < summary->minblocks) summary->minblocks=status->st_blocks;
     if (status->st_blocks > summary->maxblocks) summary->maxblocks=status->st_blocks;
  }
  if (!strncmp(type,"l",1)) {
     summary->totlinks++;
  }
  if (status->st_uid < summary->minuid) summary->minuid=status->st_uid;
  if (status->st_uid > summary->maxuid) summary->maxuid=status->st_uid;
  if (status->st_gid < summary->mingid) summary->mingid=status->st_gid;
  if (status->st_gid > summary->maxgid) summary->maxgid=status->st_gid;
  if (status->st_ctime < summary->minctime) summary->minctime=status->st_ctime;
  if (status->st_ctime > summary->maxctime) summary->maxctime=status->st_ctime;
  if (status->st_mtime < summary->minmtime) summary->minmtime=status->st_mtime;
  if (status->st_mtime > summary->maxmtime) summary->maxmtime=status->st_mtime;
  if (status->st_atime < summary->minatime) summary->minatime=status->st_atime;
  if (status->st_atime > summary->maxatime) summary->maxatime=status->st_atime;
  if (xattrs > 0) summary->totxattr++;
  return 0;
}

int tsumit (struct sum *sumin,struct sum *smout) {

// CREATE TABLE treesummary(totsubdirs INT, maxsubdirfiles INT, maxsubdirlinks INT, maxsubdirsize INT, totfiles INT, totlinks INT, minuid INT, maxuid INT, mingid INT, maxgid INT, minsize INT, maxsize INT, totltk INT, totmtk INT, totltm INT, totmtm INT, totmtg INT, totmtt INT, totsize INT, minctime INT, maxctime INT, minmtime INT, maxmtime INT, minatime INT, maxatime INT, minblocks INT, maxblocks INT, totxattr INT,depth INT);";

  smout->totsubdirs++;
  if (sumin->totfiles > smout->maxsubdirfiles) smout->maxsubdirfiles=sumin->totfiles;
  if (sumin->totlinks > smout->maxsubdirlinks) smout->maxsubdirlinks=sumin->totlinks;
  if (sumin->totsize  > smout->maxsubdirsize) smout->maxsubdirsize=sumin->totsize;
  smout->totfiles=smout->totfiles+sumin->totfiles;
  smout->totlinks=smout->totlinks+sumin->totlinks;
  smout->totsize=smout->totsize+sumin->totsize;
  /* only set these mins and maxes if there is files in the directory otherwise mins are all zero */
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
  }
  smout->totltk=smout->totltk+sumin->totltk;
  smout->totmtk=smout->totmtk+sumin->totmtk;
  smout->totltm=smout->totltm+sumin->totltm;
  smout->totmtm=smout->totmtm+sumin->totmtm;
  smout->totmtg=smout->totmtg+sumin->totmtg;
  smout->totmtt=smout->totmtt+sumin->totmtt;
  smout->totsize=smout->totsize+sumin->totsize;
  smout->totxattr=smout->totxattr+sumin->totxattr;
  return 0;
}

// given a possibly-multi-level path of directories (final component is
// also a dir), create the parent dirs all the way down.
// 
int mkpath(char* file_path, mode_t mode) {
  char* p;
  char sp[MAXPATH];

  sprintf(sp,"%s",file_path);
  for (p=strchr(file_path+1, '/'); p; p=strchr(p+1, '/')) {
    //printf("mkpath mkdir file_path %s p %s\n",file_path,p);
    *p='\0';
    //printf("mkpath mkdir file_path %s\n",file_path);
    if (mkdir(file_path, mode)==-1) {
      if (errno!=EEXIST) { *p='/'; return -1; }
    }
    *p='/';
  }
  //printf("mkpath mkdir sp %s\n",sp);
  mkdir(sp,mode);
  return 0;
}

// copy directory <name>, with all perms, times, xattrs, etc, to the
// corresponding spot under <nameto>.
//
// <xattr> is a buf holding a sequence of null-terminated strings, such as
//     would be returned by listxattr() / getxattr()
//
// <xattrs> is the number of strings in <xattr>
//
int dupdir(const char        *name,
           char              *nameto,
           const struct stat *status,
           int                xattrs,
           char              *xattr)
{
    char topath[MAXPATH];
    struct stat teststat;
    struct utimbuf ut;
    char buf[MAXXATTR];
    char bufv[MAXXATTR];
    char *xattrp;
    int cnt;
    int rc;
    
    //printf("in dupdir %s/%s",nameto,name);
    sprintf(topath,"%s/%s",nameto,name);
    //printf("mkdir %s\n",topath);
    /********* the writer must be able to create the index files into this directory so or in S_IWRITE *******/
    rc = mkdir(topath,status->st_mode | S_IWRITE);
    //rc = mkdir(topath,status->st_mode);
    if (rc != 0) {
      //perror("mkdir");
      if (errno == ENOENT) {
        //printf("calling mkpath on %s\n",topath);
        mkpath(topath,status->st_mode);
      } else if (errno == EEXIST) {
        return 0;
      } else {
        return 1;
      }
    }
    chown(topath, status->st_uid,status->st_gid);
    //printf("in dupdir xattrs %d xattr %s\n",xattrs,xattr);
    cnt = 0;
    xattrp=xattr;
    if (xattrs > 0) {
      while (cnt < xattrs) {
        bzero(buf,sizeof(buf));
        bzero(bufv,sizeof(bufv));
        strcpy(buf,xattrp); xattrp=xattrp+strlen(buf)+1;
        //printf("in dupdir key buf %s",buf); 
        strcpy(bufv,xattrp); xattrp=xattrp+strlen(bufv)+1; 
        //printf("val bufv %s\n",bufv); 

        SETXATTR(topath, buf, bufv, strlen(bufv));

        cnt++;
      }
    }
    /* debuging why utime isnt working consistently ??????????? 
    printf("passed in %s utime %ld \n",name,status->st_mtime);
    stat(name,&teststat);
    printf("src %s utime %ld \n",name,teststat.st_mtime);
    stat(topath,&teststat);
    printf("before %s utime %ld \n",topath, teststat.st_mtime);
    ut.modtime=44;
    */
    ut.modtime=status->st_mtime;
    ut.actime=status->st_atime;
    if(utime(topath,&ut)) {
      perror("utime:");
      printf("utime error name %s\n",topath);;
    }
    /* debugging why utime isnt working all the time ?????? 
    printf("setting times for %s mt %ld \n",topath, ut.modtime);
    stat(topath,&teststat);
    printf("after %s utime %ld\n",topath,teststat.st_mtime);
    */
    /**???????? i dont know if this utime thing is working ??????????*/
    return 0;
}
