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



/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <sqlite3.h>
#include <sys/xattr.h>

#include <bf.h>
#include <dbutils.h>

char globalmnt[MAXPATH];
int  globalmntlen;
char globaldbname[MAXPTHREAD][MAXPATH];
int  globaldbs;
//sqlite3 *globaldb;
char globalsql[MAXSQL];
char globaltab[MAXSQL];
char globalview[MAXSQL];
struct stat globalst;

static int gufir_getattr(const char *path, struct stat *stbuf) {
        int rc;
        sqlite3 *mydb;
        sqlite3_stmt    *res;
        const char   *tail;
        int rec_count;
        char sqlstmt[MAXSQL];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char tpath[MAXPATH];
        char dbn[MAXPATH];

        SNPRINTF(tpath,MAXPATH,"%s%s",globalmnt,path);
        p=&tpath[0]+strlen(tpath)-1;
        if (!strncmp(p,"/",1)) bzero(p,1);
        shortpath(tpath,shortpathc,endpath);
        //fprintf(stderr,"getattr %s/%s tpath %s endpath %s\n",shortpathc,DBNAME,tpath,endpath);
        //fprintf(stderr,"getattr: lstat tpath %s\n",tpath);
        SNPRINTF(dbn,MAXPATH,"%s/%s",tpath,DBNAME);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          //fprintf(stderr, "getattr SQL error on open: name %s err %s\n",dbn,sqlite3_errmsg(mydb));
          //return -EIO;
          //must not be a directory just to to files
          goto skiptofiles;
        }
        SNPRINTF(sqlstmt,MAXSQL,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from summary where name='%s';",endpath);
        //fprintf(stderr,"dir select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from entries where name='%s';",endpath);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          //fprintf(stderr, "getattr dir SQL error on query: %s name %s \n",sqlstmt,dbn);
          //return -1;
          //must not be a directory just to to files
          sqlite3_finalize(res);
          closedb(mydb);
          goto skiptofiles;
        }
        rec_count = 0;
        while (sqlite3_step(res) == SQLITE_ROW) {
          //ncols=sqlite3_column_count(res);
          stbuf->st_dev=1;
          stbuf->st_rdev=1;
#ifdef BSDSTAT
          stbuf->st_flags=0;
#endif
          stbuf->st_mode=sqlite3_column_int64(res,0);
          stbuf->st_nlink=sqlite3_column_int64(res,1);
          stbuf->st_ino=sqlite3_column_int64(res,2);
          stbuf->st_uid=sqlite3_column_int64(res,3);
          stbuf->st_gid=sqlite3_column_int64(res,4);
          stbuf->st_size=sqlite3_column_int64(res,5);
          stbuf->st_atime=sqlite3_column_int64(res,6);
          stbuf->st_mtime=sqlite3_column_int64(res,7);
          stbuf->st_ctime=sqlite3_column_int64(res,8);
          stbuf->st_blksize=sqlite3_column_int64(res,9);
          stbuf->st_blocks=sqlite3_column_int64(res,10);
          rec_count++;
          break;
        }
        sqlite3_finalize(res);
        closedb(mydb);

        if (rec_count  > 0) return 0;

skiptofiles:
        SNPRINTF(dbn,MAXPATH,"%s/%s",shortpathc,DBNAME);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "getattr SQL error on open: name %s err %s\n",dbn,sqlite3_errmsg(mydb));
          return -EIO;
        }
        if (strlen(globalsql) > 0) {
          SNPRINTF(sqlstmt,MAXSQL,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from entries where name='%s' and %s;",endpath,globalsql);
        } else {
          SNPRINTF(sqlstmt,MAXSQL,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from entries where name='%s';",endpath);
        }
        //fprintf(stderr,"getattr %s",sqlstmt);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s name %s \n",sqlstmt,dbn);
          return -1;
        }
        rec_count = 0;

        while (sqlite3_step(res) == SQLITE_ROW) {
          //ncols=sqlite3_column_count(res);
          stbuf->st_dev=1;
          stbuf->st_rdev=1;
#ifdef BSDSTAT
          stbuf->st_flags=0;
#endif
          stbuf->st_mode=sqlite3_column_int64(res,0);
          stbuf->st_nlink=sqlite3_column_int64(res,1);
          stbuf->st_ino=sqlite3_column_int64(res,2);
          stbuf->st_uid=sqlite3_column_int64(res,3);
          stbuf->st_gid=sqlite3_column_int64(res,4);
          stbuf->st_size=sqlite3_column_int64(res,5);
          stbuf->st_atime=sqlite3_column_int64(res,6);
          stbuf->st_mtime=sqlite3_column_int64(res,7);
          stbuf->st_ctime=sqlite3_column_int64(res,8);
          stbuf->st_blksize=sqlite3_column_int64(res,9);
          stbuf->st_blocks=sqlite3_column_int64(res,10);
          rec_count++;
          break;
        }
        sqlite3_finalize(res);
        closedb(mydb);
        if (rec_count  < 1) return -ENOENT;

        return 0;
}

static int gufir_readdir(const char *path, void *buf, fuse_fill_dir_t filler,off_t offset, struct fuse_file_info *fi) {
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        int rec_count;
        char sqlstmt[MAXSQL];
        char *p;
        char shortpathc[MAXPATH];
        struct dirent des;
        struct stat stbuc;
        char ttype[2];
        sqlite3 *mydb;
        DIR *dp;
        struct dirent *de;
        char tpath[MAXPATH];
        char endpath[MAXPATH];
        char dbn[MAXPATH];

        //sprintf(tpath,"%s/%s",globalmnt,path);
        SNPRINTF(tpath,MAXPATH,"%s%s",globalmnt,path);
        p=&tpath[0]+strlen(tpath)-1;
        if (!strncmp(p,"/",1)) bzero(p,1);
        shortpath(tpath,shortpathc,endpath);

        rec_count=0;
        dp = opendir(tpath);
        if (dp == NULL)
                return -errno;
        while ((de = readdir(dp)) != NULL) {
                memset(&stbuc, 0, sizeof(stbuc));
                stbuc.st_ino = de->d_ino;
                stbuc.st_mode = de->d_type << 12;
                rec_count++;
                if (de->d_type == DT_DIR) {
                  if (filler(buf, de->d_name, &stbuc, 0))
                        break;
                }
        }
        closedir(dp);

        //shortpath(tpath,shortpathc,endpath);
        //sprintf(dbn,"%s/%s",shortpathc,DBNAME);
        SNPRINTF(dbn,MAXPATH,"%s/%s",tpath,DBNAME);
        //fprintf(stderr,"readdir: opendb %s",dbn);
        if (strlen(globalsql) > 0) {
          SNPRINTF(sqlstmt,MAXSQL,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks,name,type from entries where %s;",globalsql);
        } else {
          SNPRINTF(sqlstmt,MAXSQL,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks,name,type from entries;");
        }
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "getattr SQL error on open: %s name %s err %s\n",sqlstmt,dbn,sqlite3_errmsg(mydb));
          return -EIO;
        }
        //fprintf(stderr,"readdir lookup %s\n",sqlstmt);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "SQL error on readdir query: %s name %s \n",sqlstmt,dbn);
          return -1;
        }

        while (sqlite3_step(res) == SQLITE_ROW) {
          //printf("in sql step loop %s\n",sqlite3_column_text(res,0));
          stbuc.st_dev=1;
          stbuc.st_rdev=1;
#ifdef BSDSTAT
          stbuc.st_flags=0;
#endif
          stbuc.st_mode=sqlite3_column_int64(res,0);
          stbuc.st_nlink=sqlite3_column_int64(res,1);
          stbuc.st_ino=sqlite3_column_int64(res,2);
          stbuc.st_uid=sqlite3_column_int64(res,3);
          stbuc.st_gid=sqlite3_column_int64(res,4);
          stbuc.st_size=sqlite3_column_int64(res,5);
          stbuc.st_atime=sqlite3_column_int64(res,6);
          stbuc.st_mtime=sqlite3_column_int64(res,7);
          stbuc.st_ctime=sqlite3_column_int64(res,8);
          stbuc.st_blksize=sqlite3_column_int64(res,9);
          stbuc.st_blocks=sqlite3_column_int64(res,10);
          SNPRINTF(des.d_name,256,"%s",(const char *)sqlite3_column_text(res,11));
          SNPRINTF(ttype,2,"%s",(const char *)sqlite3_column_text(res,12));
          des.d_ino=stbuc.st_ino;
          if (!strncmp(ttype,"d",1)) des.d_type = DT_DIR;
          if (!strncmp(ttype,"f",1)) des.d_type = DT_REG;
          if (!strncmp(ttype,"l",1)) des.d_type = DT_LNK;
          //printf("in sql step mode %hu nlink %d inode %lld uid %d gid %d size %lld atime %ld ctime %ld mtime %ld blksize %d blocks %lld\n",stbuf->st_mode,stbuf->st_nlink,stbuf->st_ino,stbuf->st_uid,stbuf->st_gid,stbuf->st_size,stbuf->st_atime,stbuf->st_ctime,stbuf->st_mtime, stbuf->st_blksize, stbuf->st_blocks);
          rec_count++;

          filler(buf, des.d_name, &stbuc, 0);
        }
        if (rec_count  < 1) return -ENOENT;
        sqlite3_finalize(res);
        closedb(mydb);

        return 0;
}

static int gufir_access(const char *path, int mask) {
	int res;
        struct stat stbuf;
        char dbn[MAXPATH];
        char tpath[MAXPATH];
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        int rc;
        char *p;

        //sprintf(tpath,"%s/%s",globalmnt,path);
        SNPRINTF(tpath,MAXPATH,"%s%s",globalmnt,path);
        p=&tpath[0]+strlen(tpath)-1;
        if (!strncmp(p,"/",1)) bzero(p,1);

        //fprintf(stderr,"access: lstat  and access on %s\n",tpath);
        rc = lstat(tpath, &stbuf);
        if (rc != -1) {
            if (S_ISDIR(stbuf.st_mode)) {
                res = access(tpath, mask);
                if (res == 0) {
                    return 0;
                }
            }
        }

        shortpath(tpath,shortpathc,endpath);
        SNPRINTF(dbn,MAXPATH,"%s/%s",shortpathc,DBNAME);
        //fprintf(stderr,"access: calling access on %s/%s",shortpathc,DBNAME);
        res = access(dbn, mask);
        if (res == 0) {
          return 0;
        }
	return -1;
}

static int gufir_statfs(const char *path, struct statvfs *stbuf) {
	int res;

	res = statvfs("/", stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int gufir_getxattr(const char *path, const char *name, char *value,size_t size) {
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        char sqlstmt[MAXSQL];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char txattr[MAXXATTR];
        sqlite3 *mydb;
        int rec_count;
        char tpath[MAXPATH];
        char dbn[MAXPATH];

        /* if they call with zero size tell them how big it will be padded by 1 */
        if (size==0) return MAXXATTR;

        SNPRINTF(tpath,MAXPATH,"%s%s",globalmnt,path);
        p=&tpath[0]+strlen(tpath)-1;
        if (!strncmp(p,"/",1)) bzero(p,1);
        shortpath(tpath,shortpathc,endpath);

        //fprintf(stderr,"getattr %s %s tpath %s endpath %s\n",shortpathc,DBNAME,tpath,endpath);
        //fprintf(stderr,"getxattr: lstat tpath %s\n",tpath);
        SNPRINTF(dbn,MAXPATH,"%s/%s",tpath,DBNAME);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          //must not be a directory just to to files
          goto skiptofilesgx;
        }

        SNPRINTF(sqlstmt,MAXSQL,"select xattrs from summary where name='%s';",endpath);
        //fprintf(stderr,"dir select xattrs from summary where name='%s';",endpath);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          //must not be a directory just to to files
          sqlite3_finalize(res);
          closedb(mydb);
          goto skiptofilesgx;
        }
        rec_count = 0;
        bzero(txattr,sizeof(txattr));
        while (sqlite3_step(res) == SQLITE_ROW) {
            SNPRINTF(txattr,MAXXATTR,"%s",sqlite3_column_text(res,0));
          //printf("getxattr found %s",txattr);
          rec_count++;
          break;
        }
        sqlite3_finalize(res);
        closedb(mydb);

        if (rec_count  > 0) goto finishgx;

skiptofilesgx:

        SNPRINTF(dbn,MAXPATH,"%s/%s",shortpathc,DBNAME);
        //fprintf(stderr,"getxattr %s/%s tpath %s endpath %s\n",shortpathc,DBNAME,tpath,endpath);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "getxattr SQL error on open: name %s err %s\n",dbn,sqlite3_errmsg(mydb));
          return -EIO;
        }
        if (strlen(globalsql) > 0) {
          SNPRINTF(sqlstmt,MAXSQL,"select xattrs from entries where name='%s' and %s;",endpath,globalsql);
        } else {
          SNPRINTF(sqlstmt,MAXSQL,"select xattrs from entries where name='%s';",endpath);
        }

        //fprintf(stderr,"%s",sqlstmt);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "getxattr SQL error on query: %s name %s \n",sqlstmt,dbn);
          return -1;
        }
        rec_count = 0;

        bzero(txattr,sizeof(txattr));
        while (sqlite3_step(res) == SQLITE_ROW) {
          SNPRINTF(txattr,MAXXATTR,"%s",sqlite3_column_text(res,0));
          //printf("getxattr found %s",txattr);
          rec_count++;
          break;
          }
        sqlite3_finalize(res);
        closedb(mydb);
finishgx:
        if (rec_count  < 1) {
          size=0;
          return 0;
        }
        if (size < strlen(txattr)+1) {
           /* not enough space to store */
          size=0;
          return EINVAL;
        }
        sprintf(value,"%s",txattr);
        size=strlen(txattr)+1;
        return  strlen(txattr);
}

static int gufir_listxattr(const char *path, char *list, size_t size) {
        char myattr[256];
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        int rec_count;
        char sqlstmt[MAXSQL];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char txattr[MAXXATTR];
        char tpath[MAXXATTR];
        sqlite3 *mydb;
        char dbn[MAXPATH];

        SNPRINTF(myattr,256,"xattrs");
        /* if they call with zero size tell them how big it will be padded by 1 */
        if (size==0) return (strlen(myattr)+1);

        SNPRINTF(tpath,MAXPATH,"%s%s",globalmnt,path);
        p=&tpath[0]+strlen(tpath)-1;
        if (!strncmp(p,"/",1)) bzero(p,1);
        shortpath(tpath,shortpathc,endpath);

        //fprintf(stderr,"listattr %s %s tpath %s endpath %s\n",shortpathc,DBNAME,tpath,endpath);
        //fprintf(stderr,"listxattr: lstat tpath %s\n",tpath);
        SNPRINTF(dbn,MAXPATH,"%s/%s",tpath,DBNAME);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          //must not be a directory just to to files
          goto skiptofileslx;
        }

        SNPRINTF(sqlstmt,MAXSQL,"select xattrs from summary where name='%s';",endpath);
        //fprintf(stderr,"listxattr dir select xattrs from summary where name='%s';",endpath);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          //must not be a directory just to to files
          sqlite3_finalize(res);
          closedb(mydb);
          goto skiptofileslx;
        }
        rec_count = 0;
        memset(txattr, 0, sizeof(txattr));
        while (sqlite3_step(res) == SQLITE_ROW) {
          SNPRINTF(txattr,MAXXATTR,"%s",sqlite3_column_text(res,0));
          //printf("listxattr found %s",txattr);
          rec_count++;
          break;
        }
        sqlite3_finalize(res);
        closedb(mydb);

        if (rec_count  > 0) goto finishlx;

skiptofileslx:

        SNPRINTF(dbn,MAXPATH,"%s/%s",shortpathc,DBNAME);
        //fprintf(stderr,"listxattr %s/%s tpath %s endpath %s\n",shortpathc,DBNAME,tpath,endpath);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "listxattr SQL error on open: name %s err %s\n",dbn,sqlite3_errmsg(mydb));
          return -EIO;
        }
        if (strlen(globalsql) > 0) {
          SNPRINTF(sqlstmt,MAXSQL,"select xattrs from entries where name='%s' and %s;",endpath,globalsql);
        } else {
          SNPRINTF(sqlstmt,MAXSQL,"select xattrs from entries where name='%s';",endpath);
        }
        //fprintf(stderr,"%s",sqlstmt);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "listxattr SQL error on query: %s name %s \n",sqlstmt,dbn);
          return -1;
        }
        rec_count = 0;

        bzero(txattr,sizeof(txattr));
        while (sqlite3_step(res) == SQLITE_ROW) {
          SNPRINTF(txattr,MAXXATTR,"%s",sqlite3_column_text(res,0));
          //printf("getxattr found %s",txattr);
          rec_count++;
          break;
          }
        sqlite3_finalize(res);
        closedb(mydb);
finishlx:
        SNPRINTF(txattr,MAXXATTR,"%s",myattr);
        if (rec_count  < 1) {
          size=0;
          return 0;
        }
        if (size < strlen(txattr)+1) {
           /* not enough space to store */
          size=0;
          return -1;
        }
        sprintf(list,"%s",txattr);
        size=strlen(txattr)+1;
        return  strlen(txattr)+1;
}

static int gufir_readlink(const char *path, char *buf, size_t size) {
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        int rec_count;
        char sqlstmt[MAXSQL];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char treadlink[MAXPATH];
        sqlite3 *mydb;
        char tpath[MAXXATTR];
        char dbn[MAXPATH];

        /* if they call with zero size tell them how big it will be padded by 1 */
        if (size==0) return (MAXPATH);

        SNPRINTF(tpath,MAXXATTR,"%s%s",globalmnt,path);
        p=&tpath[0]+strlen(tpath)-1;
        if (!strncmp(p,"/",1)) bzero(p,1);
        shortpath(tpath,shortpathc,endpath);

        //fprintf(stderr,"readlink %s %s tpath %s endpath %s\n",shortpathc,DBNAME,tpath,endpath);
        //fprintf(stderr,"readlink: lstat tpath %s\n",tpath);
        SNPRINTF(dbn,MAXPATH,"%s/%s",tpath,DBNAME);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          //must not be a directory just to to files
          goto skiptofilesl;
        }

        SNPRINTF(sqlstmt,MAXSQL,"select linkname from summary where name='%s';",endpath);
        //fprintf(stderr,"readlink dir select xattrs from summary where name='%s';",endpath);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          //must not be a directory just to to files
          sqlite3_finalize(res);
          closedb(mydb);
          goto skiptofilesl;
        }
        rec_count = 0;
        bzero(treadlink,sizeof(treadlink));
        while (sqlite3_step(res) == SQLITE_ROW) {
          SNPRINTF(treadlink,MAXPATH,"%s",sqlite3_column_text(res,0));
          //printf("readlink found %s",treadlink);
          rec_count++;
          break;
        }
        sqlite3_finalize(res);
        closedb(mydb);

        if (rec_count  > 0) goto finishl;

skiptofilesl:

        SNPRINTF(dbn,MAXPATH,"%s/%s",shortpathc,DBNAME);
        //fprintf(stderr,"readlink %s/%s tpath %s endpath %s\n",shortpathc,DBNAME,tpath,endpath);
        rc = sqlite3_open(dbn, &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "readlink SQL error on open: name %s err %s\n",dbn,sqlite3_errmsg(mydb));
          return -EIO;
        }
        if (strlen(globalsql) > 0) {
          SNPRINTF(sqlstmt,MAXSQL,"select linkname from entries where name='%s' and %s;",endpath,globalsql);
        } else {
          SNPRINTF(sqlstmt,MAXSQL,"select linkname from entries where name='%s';",endpath);
        }
        //fprintf(stderr,"%s",sqlstmt);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "readlink SQL error on query: %s name %s \n",sqlstmt,dbn);
          return -1;
        }
        rec_count = 0;

        bzero(treadlink,sizeof(treadlink));
        while (sqlite3_step(res) == SQLITE_ROW) {
          SNPRINTF(treadlink,MAXPATH,"%s",sqlite3_column_text(res,0));
          //printf("readlink found %s",treadlink);
          rec_count++;
          break;
          }
        sqlite3_finalize(res);
        closedb(mydb);
finishl:
        if (rec_count  < 1) {
          size=0;
          return 0;
        }
        if (size < strlen(treadlink)+1) {
           /* not enough space to store */
          size=0;
          return -1;
        }
        sprintf(buf,"%s",treadlink);
        size=strlen(treadlink)+1;
        return  (strlen(treadlink)+1);
}

static struct fuse_operations gufir_oper = {
    .getattr    = gufir_getattr,
    .readdir    = gufir_readdir,
    .access     = gufir_access,
    .statfs     = gufir_statfs,
    .listxattr  = gufir_listxattr,
    .getxattr   = gufir_getxattr,
    .readlink   = gufir_readlink,
};

int main(int argc, char *argv[]) {
        sqlite3 *mydb;
        char indbname[MAXPATH];
        int rc;
        char sqlu[MAXSQL];
        sqlite3_stmt *res;
        const char   *tail;


        //printf("argc %d argv0 %s argv1 %s argv2 %s argv3 %s\n",argc,argv[0],argv[1],argv[2],argv[3]);
        //last arg is sql where clause
        SNPRINTF(globalsql,MAXSQL,"%s",argv[argc-1]);
        argc--;
        // next to last a gufi directory where you are making your top dir
        SNPRINTF(globalmnt,MAXPATH,"%s",argv[argc-1]);
        argc--;

        SNPRINTF(indbname,MAXPATH,"%s/%s",globalmnt,DBNAME);
        rc = sqlite3_open(indbname, &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "main SQL error on open:  err %s\n",sqlite3_errmsg(mydb));
          exit(-1);
        }

        SNPRINTF(sqlu,MAXSQL,"select name from summary limit 1;");
        rc = sqlite3_prepare_v2(mydb, sqlu, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "SQL error on testing to see if the top dir is a gufi dir: %s err %s\n",sqlu,sqlite3_errmsg(mydb));
          exit(-1);
        }
        if (sqlite3_step(res) != SQLITE_ROW) {
          fprintf(stderr, "SQL error in main testing to see if we are a gufi\n");
          exit(-1);
        }
        sqlite3_finalize(res);
        closedb(mydb);

	return fuse_main(argc, argv, &gufir_oper, NULL);

}
