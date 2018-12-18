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

#include "bf.h"
#include "dbutils.h"

char globalmnt[MAXPATH];
int  globalmntlen;
char globaldbname[MAXPTHREAD][MAXPATH];
int  globaldbs;
//sqlite3 *globaldb;
char globaltab[MAXSQL];
char globalview[MAXSQL];
struct stat globalst;

int att(sqlite3 *indb) {
        sqlite3 *db;
        int i;
        char indbname[MAXPATH];
        char qat[MAXSQL];
        int rc;
        char *err_msg = 0;
        char sqlu[MAXSQL];
        char up[MAXSQL];

        bzero(sqlu,sizeof(sqlu));
        sprintf(sqlu,"create temp view %s as ",globalview);
        i=0;
        while (i<globaldbs) {
          sprintf(qat,"ATTACH \'%s\' as mdb%d;",globaldbname[i],i);
          rc=sqlite3_exec(indb, qat,0, 0, &err_msg);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "Cannot attach database: %s %s\n", sqlite3_errmsg(db),globaldbname[i]);
            sqlite3_close(db);

            return -EIO;  // exit(9);
          }
          sprintf(up,"select * from mdb%d.%s",i,globaltab);
          strcat(sqlu,up);
          if (globaldbs > 1) {
            if (i < (globaldbs-1)) {
              sprintf(up," union all ");
              strcat(sqlu,up);
            } 
            i++;
          }
        }
        strcat(sqlu,";");

        //printf("sqlu: %s\n",sqlu);
        rawquerydb(indbname, 0, indb, sqlu,0, 0, 0, 0);
        return 0;
}

static int gufir_getattr(const char *path, struct stat *stbuf) {
        int rc;
        sqlite3 *mydb;
        sqlite3_stmt    *res;
        const char   *tail;
        int rec_count;
        char sqlstmt[1024];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];

        if (strlen(path) <= globalmntlen) {
          /* input path is shorter or equal (equal because this is a stat than shortest db path */
          if (!strncmp(globalmnt,path,strlen(path))) {
            p=globalmnt+strlen(path);
            //fprintf(stderr,"pointer %s\n",p); 
            if (strlen(path) > 1) {
              if (strlen(path) != globalmntlen) {
                if (strncmp(p,"/",1)) return -ENOENT; 
              }
            }
            /* just stuff in stat stucture from cwd - sure its cheating but we dont know anyting about this dir really */ 
            stbuf->st_ino=globalst.st_ino;
            stbuf->st_nlink=globalst.st_nlink;
            stbuf->st_size=globalst.st_size;
            stbuf->st_uid=globalst.st_uid;
            stbuf->st_gid=globalst.st_gid;
            stbuf->st_mode=globalst.st_mode;
            stbuf->st_ctime=globalst.st_ctime;
            stbuf->st_mtime=globalst.st_mtime;
            stbuf->st_atime=globalst.st_atime;
            stbuf->st_blksize=globalst.st_blksize;
            stbuf->st_blocks=globalst.st_blocks;
            //fprintf(stderr,"getattr path less than smallest db %s %s\n",path,globalmnt);
            //fprintf(stderr,"getattr uid %ul gid %ul ctime %ld\n",stbuf->st_uid,stbuf->st_gid,stbuf->st_ctime);
            return 0;
          }
          /* if input path not same as the first part of the shortest db path then this is a bogus request */
          return -ENOENT;
        } else {
          /* input path is equal to or longer than the shortest db path meaning we should just look up for an exact match */

          //mydb = opendb(":memory",db,5,0);
          //rc = sqlite3_open(":memory", &mydb);
          rc = sqlite3_open("", &mydb);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "getattr SQL error on open: %s name %s err %s\n",sqlstmt,path,sqlite3_errmsg(mydb));
            return -EIO;
          }
          att(mydb); 
          shortpath(path,shortpathc,endpath);
          sprintf(sqlstmt,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from %s where fullpath='%s' and name='%s';",globalview,shortpathc,endpath); 
          rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "SQL error on query: %s name %s \n",sqlstmt,path);
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
        }     

        return 0;
}

static int gufir_readdir(const char *path, void *buf, fuse_fill_dir_t filler,off_t offset, struct fuse_file_info *fi) {
        int i;
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        int rec_count;
        char sqlstmt[1024];
        char *p;
        char shortpathc[MAXPATH];
        struct dirent des;
        struct stat stbuc;
        char ttype[2];
        sqlite3 *mydb;

        if (strlen(path) < globalmntlen) {
          /* input path is shorter than shortest db path */
          if (!strncmp(globalmnt,path,strlen(path))) {
            p=globalmnt+strlen(path);
            //fprintf(stderr,"pointer %s\n",p); 
            if (strlen(path) > 1) {
              if (strlen(path) != globalmntlen) {
                if (strncmp(p,"/",1)) return -ENOENT; 
              }
            } else {
                p=globalmnt;
            }
            /* just stuff in stat stucture from cwd - sure its cheating but we dont know anyting about this dir really */ 
            memset(&stbuc, 0, sizeof(stbuc));
            stbuc.st_ino=globalst.st_ino;
            stbuc.st_nlink=globalst.st_nlink;
            stbuc.st_size=globalst.st_size;
            stbuc.st_uid=globalst.st_uid;
            stbuc.st_gid=globalst.st_gid;
            stbuc.st_mode=globalst.st_mode;
            stbuc.st_ctime=globalst.st_ctime;
            stbuc.st_mtime=globalst.st_mtime;
            stbuc.st_atime=globalst.st_atime;
            stbuc.st_blksize=globalst.st_blksize;
            stbuc.st_blocks=globalst.st_blocks;
            des.d_ino=globalst.st_ino;
            des.d_type = DT_DIR ;
            /* pull the next level directory out of the globalmnt as the only entry in the directory being readdird */
            sprintf(shortpathc,"%s",p+1);
            for(i = 0; i <= strlen(shortpathc); i++) {
  		if(shortpathc[i] == '/')  
		{
                        bzero(&shortpathc[i],1);
			break;    	
 		}
            }
            sprintf(des.d_name,"%s",shortpathc);
            //fprintf(stderr,"readdir path less than smallest db %s %s\n",path,globalmnt);
            //fprintf(stderr,"readdir uid %ul gid %ul ctime %ld\n",stbuc.st_uid,stbuc.st_gid,stbuc.st_ctime);

            filler(buf, des.d_name, &stbuc, 0);

            return 0;

          }
          /* if input path not same as the first part of the shortest db path then this is a bogus request */
          return -ENOENT;
        } else {
          /* input path is equal to or longer than the shortest db path meaning we should just look up for an exact match */

          sprintf(sqlstmt,"select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks,name,type from %s where fullpath='%s';",globalview,path); 
          rc = sqlite3_open("", &mydb);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "getattr SQL error on open: %s name %s err %s\n",sqlstmt,path,sqlite3_errmsg(mydb));
            return -EIO;
          }
          att(mydb); 
          //fprintf(stderr,"readdir lookup %s\n",sqlstmt); 
          rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "SQL error on readdir query: %s name %s \n",sqlstmt,path);
            return -1;
          }
          rec_count = 0;
               
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
            sprintf(des.d_name,"%s",(const char *)sqlite3_column_text(res,11));
            sprintf(ttype,"%s",(const char *)sqlite3_column_text(res,12));
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
        }     

        return 0;
}

static int gufir_access(const char *path, int mask) {
	int res;
        
        res=0;
	return 0;
}

static int gufir_statfs(const char *path, struct statvfs *stbuf) {
	int res;

	res = statvfs("/", stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef __APPLE__
static int gufir_getxattr(const char *path, const char *name, char *value, size_t size, uint32_t position)
#else
static int gufir_getxattr(const char *path, const char *name, char *value, size_t size)
#endif
{
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        int     error = 0;
        int rec_count;
        char sqlstmt[1024];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char txattr[MAXXATTR];
        sqlite3 *mydb;

        /* if they call with zero size tell them how big it will be padded by 1 */
        if (size==0) return MAXXATTR; 

        if (strlen(path) <= globalmntlen) {
          if (!strncmp(globalmnt,path,strlen(path))) {
            p=globalmnt+strlen(path);
            //fprintf(stderr,"pointer %s\n",p); 
            if (strlen(path) > 1) {
              if (strlen(path) != globalmntlen) {
                if (strncmp(p,"/",1)) return -EINVAL; 
              }
            }
            size=0;
            //fprintf(stderr,"getxattr path less than smallest db %s %s\n",path,globalmnt);
            return 0;
          }
            return -EINVAL;
        } else {

          shortpath(path,shortpathc,endpath);
          sprintf(sqlstmt,"select xattrs from %s where fullpath='%s' and name='%s';",globalview,shortpathc,endpath); 
          rc = sqlite3_open("", &mydb);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "getattr SQL error on open: %s name %s err %s\n",sqlstmt,path,sqlite3_errmsg(mydb));
            return -EIO;
          }
          att(mydb); 
          //fprintf(stderr,"getxattr lookup %s\n",sqlstmt); 
          error = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
          if (error != SQLITE_OK) {
            fprintf(stderr, "SQL error on query: %s name %s \n",sqlstmt,path);
            return -1;
          }
          rec_count = 0;
          bzero(txattr,sizeof(txattr)); 
          //printf("getxattr issuing query\n");
          while (sqlite3_step(res) == SQLITE_ROW) {
            //printf("in sql step loop %s\n",sqlite3_column_text(res,0));
            //ncols=sqlite3_column_count(res);
            sprintf(txattr,"%s",sqlite3_column_text(res,0));
            //printf("getxattr found %s",txattr);
            rec_count++;
            break;
          }
          sqlite3_finalize(res);
          closedb(mydb);
        }
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
        int     error = 0;
        int rec_count;
        char sqlstmt[1024];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char txattr[MAXXATTR];
        sqlite3 *mydb;

        sprintf(myattr,"xattrs");
        /* if they call with zero size tell them how big it will be padded by 1 */
        if (size==0) return (strlen(myattr)+1); 
        //printf("in listxattrs %s\n",path);
        if (strlen(path) <= globalmntlen) {
          if (!strncmp(globalmnt,path,strlen(path))) {
            p=globalmnt+strlen(path);
            //fprintf(stderr,"pointer %s\n",p); 
            if (strlen(path) > 1) {
              if (strlen(path) != globalmntlen) {
                if (strncmp(p,"/",1)) return -1; 
              }
            }
            size=0;
            return 0;
          }
          return -1;
        } else {

          shortpath(path,shortpathc,endpath);
          sprintf(sqlstmt,"select xattrs from %s where fullpath='%s' and name='%s';",globalview,shortpathc,endpath); 
          rc = sqlite3_open("", &mydb);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "getattr SQL error on open: %s name %s err %s\n",sqlstmt,path,sqlite3_errmsg(mydb));
            return -EIO;
          }
          att(mydb); 
          //fprintf(stderr,"getxattr lookup %s\n",sqlstmt); 
          error = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
          if (error != SQLITE_OK) {
            fprintf(stderr, "SQL error on query: %s name %s \n",sqlstmt,path);
            return -1;
          }
          rec_count = 0;
          bzero(txattr,sizeof(txattr)); 
          //printf("getxattr issuing query\n");
          while (sqlite3_step(res) == SQLITE_ROW) {
            //printf("in sql step loop %s\n",sqlite3_column_text(res,0));
            //ncols=sqlite3_column_count(res);
            sprintf(txattr,"%s",sqlite3_column_text(res,0));
            //printf("getxattr found %s",txattr);
            rec_count++;
            break;
          }
          sqlite3_finalize(res);
          closedb(mydb);
        }
        if (rec_count  < 1) {
          size=0;
          return 0;
        } 
        if (size < strlen(myattr)+1) {
           /* not enough space to store */
          size=0;
          return -1;
        }
        sprintf(list,"%s",myattr);
        size=strlen(myattr)+1;    
        return  (strlen(myattr)+1);
}

static int gufir_readlink(const char *path, char *buf, size_t size) {
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        int rec_count;
        char sqlstmt[1024];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char treadlink[MAXPATH];
        sqlite3 *mydb;

        if (strlen(path) <= globalmntlen) {
          if (!strncmp(globalmnt,path,strlen(path))) {
            p=globalmnt+strlen(path);
            //fprintf(stderr,"pointer %s\n",p); 
            if (strlen(path) > 1) {
              if (strlen(path) != globalmntlen) {
                if (strncmp(p,"/",1)) return -1; 
              }
            }
            buf[0] = '\0';
            return 0;
          }
          return -1;
        } else {

          shortpath(path,shortpathc,endpath);
          sprintf(sqlstmt,"select linkname from %s where fullpath='%s' and name='%s';",globalview,shortpathc,endpath); 
          rc = sqlite3_open("", &mydb);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "getattr SQL error on open: %s name %s err %s\n",sqlstmt,path,sqlite3_errmsg(mydb));
            return -EIO;
          }
          att(mydb); 
          //fprintf(stderr,"readlink lookup %s\n",sqlstmt); 
          rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
          if (rc != SQLITE_OK) {
            fprintf(stderr, "SQL error on query: %s name %s \n",sqlstmt,path);
            return -1;
          }
          rec_count = 0;
          bzero(treadlink,sizeof(treadlink)); 
          //printf("readlink issuing query\n");
          while (sqlite3_step(res) == SQLITE_ROW) {
            //printf("in sql step loop %s\n",sqlite3_column_text(res,0));
            //ncols=sqlite3_column_count(res);
            sprintf(treadlink,"%s",sqlite3_column_text(res,0));
            //printf("readlink found %s",treadlink);
            rec_count++;
            break;
          }
          sqlite3_finalize(res);
          closedb(mydb);
        }
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
	.getattr	= gufir_getattr,
	.readdir	= gufir_readdir,
        .access 	= gufir_access,
        .statfs 	= gufir_statfs,
	.listxattr	= gufir_listxattr,
	.getxattr	= gufir_getxattr,
        .readlink	= gufir_readlink,
};
int main(int argc, char *argv[]) {
        sqlite3 *mydb;
        int i;
        char indbname[MAXPATH];
        int rc;
        char sqlu[MAXSQL];
        sqlite3_stmt *res;
        const char   *tail;
        char cwd[MAXPATH];

 
        //printf("argc %d argv0 %s argv1 %s argv2 %s argv3 %s\n",argc,argv[0],argv[1],argv[2],argv[3]);
        // last arg has to be number of dbs
        globaldbs=atoi(argv[argc-1]);
        argc--;
        // next to last arg has to be base path/name of db files
        sprintf(indbname,"%s",argv[argc-1]);
        argc--;
        // next to next to last arg has to be the table name in the db
        sprintf(globaltab,"%s",argv[argc-1]);
        argc--;

        sprintf(globalview,"v%s",globaltab);

        i=0;
        while (i<globaldbs) {
          sprintf(globaldbname[i],"%s.%d",indbname,i);
          i++; 
        }
        //sqlite3_initialize();
        //rc=sqlite3_config(SQLITE_CONFIG_SERIALIZED);
        //sqlite3_initialize();

        //globaldb = opendb("/tmp/tmpdb",db,5,0);
        //globaldb = opendb(":memory",db,5,0);
        rc = sqlite3_open("", &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "getattr SQL error on open:  err %s\n",sqlite3_errmsg(mydb));
          exit(-1);
        }
        att(mydb);

        sprintf(sqlu,"select fullpath,length(fullpath) as pl from %s where type=\"d\" order by pl limit 1;",globalview);
        rc = sqlite3_prepare_v2(mydb, sqlu, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "SQL error on finding global path query: %s err %s\n",sqlu,sqlite3_errmsg(mydb));
          exit(-1);
        }
        if (sqlite3_step(res) != SQLITE_ROW) {
          fprintf(stderr, "SQL error on finding global path retrieval\n");
          exit(-1);
        }
        globalmntlen=0;
        sprintf(globalmnt,"%s",sqlite3_column_text(res,0));
        globalmntlen  = sqlite3_column_int64(res, 1);
        sqlite3_finalize(res);
        closedb(mydb);

        if (strncmp(globalmnt,"/",1)) {
          fprintf(stderr,"path from query resulted in it not starting with a slash %s\n",globalmnt);
          exit(-1);
        }
        if (globalmntlen <1) {
          fprintf(stderr,"path from query resulted in length < 1 %d %s\n",globalmntlen,globalmnt);
          exit(-1);
        }
        
        getcwd(cwd, MAXPATH);
        stat(cwd,&globalst);

	return fuse_main(argc, argv, &gufir_oper, NULL);

}

