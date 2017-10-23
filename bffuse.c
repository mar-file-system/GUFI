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

#define MAXPATH 1024
#define MAXXATTR 1024
#define MAXSQL 1024
#include "bf.h"
#include "dbutils.c"

//char globalmnt[1024];
//static const char *globalmnt = "/tmp/testdirdup/Users/ggrider";
static const char *globalmnt = "testdirdup/testdir";
static const char *globalsloc = "/tmp/summary.query";
static const char *globaleloc = "/tmp/entries.query";

static int hello_getattr(const char *path, struct stat *stbuf)
{       
        int rc;
        char fpath[1024];
        const char * ptr;
        sqlite3_stmt    *res;
        const char      *tail;
        const char      *errMSG;
        int     error = 0;
        sqlite3 *db;
        sqlite3 *db1;
        int rec_count;
        const char *ptrl;
        int cnt;
        char sqlstmt[1024];
        char dbpath[1024];
        struct stat st;

        ptr = path;
        if (!strncmp(path,"/",1)) ptr++;   
        sprintf(fpath,"%s/%s",globalmnt,ptr);
        //fprintf(stdout,"fpath %s\n",fpath);
        rc = lstat(fpath, stbuf);
        //printf("lstat return %d\n",rc);
        if (rc == -1) {
            if (errno == ENOENT) {
               sprintf(dbpath,"%s",fpath);
               ptr = dbpath;
               cnt = strlen(dbpath);
               ptrl = dbpath + cnt;
               while (cnt > 0) {
                 if (strncmp(ptrl,"/",1)) {
                    ptrl--; 
                 } else {
                    bzero((void *)ptrl,1); 
                    break;
                 }
                 cnt--;
               } 
               //fprintf(stderr,"in getattr opening db %s\n",dbpath);
               db = opendb(dbpath,"",&st, db1,1,0);
               ptr = fpath;
               cnt = strlen(fpath);
               ptrl = fpath + cnt;
               while (cnt > 0) {
                 if (strncmp(ptrl,"/",1)) {
                    ptrl--; 
                 } else {
                    break;
                 }
                 cnt--;
               } 

               sprintf(sqlstmt,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from entries where name='%s';",ptrl+1); 
               //fprintf(stderr,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from entries where name='%s';",ptrl+1); 
               error = sqlite3_prepare_v2(db,sqlstmt, 1000, &res, &tail);
               if (error != SQLITE_OK) {
                 fprintf(stderr, "SQL error on query: %s name %s \n",sqlstmt,fpath);
                 return -1;
               }
               rec_count = 0;
               
               //rc = lstat("/tmp/testdirdup/entries.db", stbuf);

               while (sqlite3_step(res) == SQLITE_ROW) {
                 //printf("in sql step loop %s\n",sqlite3_column_text(res,0));
                 //ncols=sqlite3_column_count(res);
                 stbuf->st_dev=1;
                 stbuf->st_rdev=1;
#ifdef BSDSTAT
                 stbuf->st_flags=0;
#endif
                 stbuf->st_mode=atol((const char *) sqlite3_column_text(res,0));
                 stbuf->st_nlink=atol((const char *)sqlite3_column_text(res,1));
                 stbuf->st_ino=atoll((const char *)sqlite3_column_text(res,2));
                 stbuf->st_uid=atol((const char *)sqlite3_column_text(res,3));
                 stbuf->st_gid=atol((const char *)sqlite3_column_text(res,4));
                 stbuf->st_size=atoll((const char *)sqlite3_column_text(res,5));
                 stbuf->st_atime=atol((const char *)sqlite3_column_text(res,6));
                 stbuf->st_mtime=atol((const char *)sqlite3_column_text(res,7));
                 stbuf->st_ctime=atol((const char *)sqlite3_column_text(res,8));
                 stbuf->st_blksize=atol((const char *)sqlite3_column_text(res,9));
                 stbuf->st_blocks=atol((const char *)sqlite3_column_text(res,10));
                 //printf("in sql step mode %hu nlink %d inode %lld uid %d gid %d size %lld atime %ld ctime %ld mtime %ld blksize %d blocks %lld\n",stbuf->st_mode,stbuf->st_nlink,stbuf->st_ino,stbuf->st_uid,stbuf->st_gid,stbuf->st_size,stbuf->st_atime,stbuf->st_ctime,stbuf->st_mtime, stbuf->st_blksize, stbuf->st_blocks);
                 rec_count++;
                 break;
               }

               sqlite3_finalize(res);
               closedb(db);
               if (rec_count  < 1) return ENOENT;
            } else {
               return -errno;
            }
        }     

        return 0;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
        DIR *dp;
        struct dirent *de;
        struct dirent des;
        char fpath[1024];

        (void) offset;
        (void) fi;
        sqlite3 *db;
        sqlite3 *sdb;
        sqlite3 *db1;
        sqlite3 *db2;
        char eesqlstmt[1024];
        char esqlstmt[1024];
        char ssqlstmt[1024];
        char sssqlstmt[1024];
        int     error = 0;
        int     rec_count = 0;
        const char      *errMSG;
        const char      *tail;
        int ncols;
        int cnt;
        int rc;
        char ctmp[1024];
        char dbn[1024];
        sqlite3_stmt    *res;
        struct stat st;
        const char * ptr;
        int fdentries;
        int fdsummary;
        int summarymatch;

        ptr = path;
        if (!strncmp(path,"/",1)) ptr++;   
        sprintf(fpath,"%s/%s",globalmnt,ptr);
        //fprintf(stderr,"in readdir: fpath %s\n",fpath);
        dp = opendir(fpath);
        if (dp == NULL)
                return -errno;

        while ((de = readdir(dp)) != NULL) {
                memset(&st, 0, sizeof(st));
                st.st_ino = de->d_ino;
                st.st_mode = de->d_type << 12;
                if (de->d_type == DT_DIR) {
                  if (filler(buf, de->d_name, &st, 0))
                        break;
                } 
        }
      
        /*??? put get sql from variable here ???*/
        db = opendb(fpath,"",&st,db1,1,0);
        sdb = opendb(fpath,"",&st,db2,0,0);
        bzero(esqlstmt,sizeof(esqlstmt));
        bzero(eesqlstmt,sizeof(eesqlstmt));
        bzero(ssqlstmt,sizeof(ssqlstmt));
        bzero(sssqlstmt,sizeof(sssqlstmt));
        summarymatch = 1;
        fdsummary = open(globalsloc, O_RDONLY); 
        if (fdsummary > 0) {
          summarymatch = 0;
          rc = pread(fdsummary, ssqlstmt, sizeof(ssqlstmt), 0);
          close(fdsummary);
          sprintf(sssqlstmt,"select name from summary %s ;",ssqlstmt); 
          //printf("summary: %s",sssqlstmt); 
          error = sqlite3_prepare_v2(sdb,sssqlstmt, 1000, &res, &tail);
          if (error != SQLITE_OK) {
            fprintf(stderr, "SQL error on query: %s name %s \n",esqlstmt,fpath);
            sqlite3_finalize(res);
            return 0;
          } 
          while (sqlite3_step(res) == SQLITE_ROW) {
            summarymatch++;
            break;
          }
          //sqlite3_finalize(res);
        } 
        if (summarymatch > 0) {
          fdentries = open(globaleloc, O_RDONLY);
          if (fdentries > 0) {
            rc = pread(fdentries, esqlstmt, sizeof(esqlstmt), 0);
            close(fdentries);
          }
          sprintf(eesqlstmt,"select name,inode,type from entries %s ;",esqlstmt); 
          //printf("entries: %s",eesqlstmt); 
          error = sqlite3_prepare_v2(db,eesqlstmt, 1000, &res, &tail);
          if (error != SQLITE_OK) {
            fprintf(stderr, "SQL error on query: %s name %s \n",esqlstmt,fpath);
            return 0;
          }
          //printf("printdir %d issummary %d\n",printdir,issummary);
          //ncols=sqlite3_column_count(res);
          //printf("query ncols %d\n",ncols);
          while (sqlite3_step(res) == SQLITE_ROW) {
             memset(&st, 0, sizeof(st));
             //printf("in sql step loop %s\n",sqlite3_column_text(res,0));
             //ncols=sqlite3_column_count(res);
             //printf("%s|", sqlite3_column_text(res, cnt));
             //printf("%s|", sqlite3_column_text(res, 0));
             sprintf(des.d_name,"%s",sqlite3_column_text(res,0));
             //printf("in sql step loop name %s\n",des.d_name);
             sprintf(ctmp,"%s",sqlite3_column_text(res,1));
             des.d_ino=atoll(ctmp);
             st.st_ino = des.d_ino;
             sprintf(ctmp,"%s",sqlite3_column_text(res,2));
             if (!strncmp(ctmp,"f",1)) des.d_type = DT_REG ;
             if (!strncmp(ctmp,"l",1)) des.d_type = DT_LNK;
             st.st_mode = des.d_type << 12;
             if (filler(buf, des.d_name, &st, 0))
                   break;
             rec_count++;
          }
        }
        sqlite3_finalize(res);
        closedb(db);
        closedb(sdb);
        closedir(dp);
        return 0;
}
/*
static int hello_open(const char *path, struct fuse_file_info *fi)
{
	if (strcmp(path, hello_path) != 0)
		return -ENOENT;
	if ((fi->flags & 3) != O_RDONLY)
		return -EACCES;
	return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
	if(strcmp(path, hello_path) != 0)
		return -ENOENT;

	len = strlen(hello_str);
	if (offset < len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, hello_str + offset, size);
	} else
		size = 0;
	return size;
}
*/
static struct fuse_operations hello_oper = {
	.getattr	= hello_getattr,
	.readdir	= hello_readdir,
//	.open		= hello_open,
//	.read		= hello_read,
};
int main(int argc, char *argv[])
{
	return fuse_main(argc, argv, &hello_oper, NULL);
}

