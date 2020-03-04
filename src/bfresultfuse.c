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

#include <bf.h>
#include <dbutils.h>

static int print(void *args, int count, char **data, char **columns) {
    for(int i = 0; i < count; i++) {
        printf("%s ", data[i]);
    }
    printf("\n");
    return 0;
}

extern int errno;
char globalmnt[MAXPATH];
size_t  globalmntlen;
char globaldbname[MAXPATH];
int  globaldbs;
//sqlite3 *globaldb;
char globaltab[MAXSQL];
char globalview[MAXSQL];
struct stat globalst;

int att(sqlite3 *db) {
    char *err = NULL;

    /* attach all database files */
    for(int i = 0; i < globaldbs; i++) {
        char attach[MAXSQL];
        SNPRINTF(attach, MAXSQL, "ATTACH '%s.%d' as mdb%d", globaldbname, i, i);

        if (sqlite3_exec(db, attach, NULL, NULL, &err) != SQLITE_OK) {
            fprintf(stderr, "Could not attach %s.%d: %s\n", globaldbname, i, err);
            sqlite3_free(err);
            return -1;
        }
    }

    /* create the view that merges all of the attached databases */
    char view[MAXSQL];
    char *vp = view;
    vp += SNPRINTF(vp, MAXSQL, "CREATE TEMP VIEW %s as SELECT * FROM mdb0.%s", globalview, globaltab);
    for(int i = 1; i < globaldbs; i++) {
        vp += SNPRINTF(vp, MAXSQL, " UNION ALL SELECT * FROM mdb%d.%s", i, globaltab);
    }

    if (sqlite3_exec(db, view, NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "Could not create view %s: %s\n", globalview, err);
        sqlite3_free(err);
        return -1;
    }

    return 0;
}

struct StatResults {
    struct stat *st;          /* stat struct provided by fuse */
    size_t count;             /* how many results have been obtained; should be 1 at the end */
};

static int fill_stat(struct stat *st, int count, char **data) {
    if (count < 11) {
        return -1;
    }

    st->st_dev=1;
    st->st_rdev=1;
#ifdef BSDSTAT
    st->st_flags=0;
#endif
    st->st_mode=atoi(data[0]);
    st->st_nlink=atoi(data[1]);
    st->st_ino=atoi(data[2]);
    st->st_uid=atoi(data[3]);
    st->st_gid=atoi(data[4]);
    st->st_size=atoi(data[5]);
    st->st_atime=atoi(data[6]);
    st->st_mtime=atoi(data[7]);
    st->st_ctime=atoi(data[8]);
    st->st_blksize=atoi(data[9]);
    st->st_blocks=atoi(data[10]);

    return 0;
}

static int stat_results(void *args, int count, char **data, char **columns) {
    struct StatResults *res = (struct StatResults *) args;

    if (count != 11) {
        return -1;
    }

    fill_stat(res->st, count, data);
    res->count++;

    return 0;
}

static int gufir_getattr(const char *path, struct stat *stbuf) {
    int rc = 0;
    sqlite3 *db = NULL;
    char tpath[MAXPATH];
    char sqlstmt[MAXSQL];
    char *p = NULL;

    if (strlen(path) < globalmntlen) {
        /* input path is shorter or equal (equal because this is a stat than shortest db path */
        if (!strncmp(globalmnt,path,strlen(path))) {
            p=globalmnt+strlen(path);
            //fprintf(stderr,"pointer %s\n",p);
            if (strlen(path) > 1) {
                if (strlen(path) != globalmntlen) {
                    if (strncmp(p,"/",1)) {
                        return -ENOENT;
                    }
                }
            }

           /* just stuff in stat stucture from cwd - sure its cheating but we dont know anyting about this dir really */
            *stbuf = globalst;
            return 0;
        }
        /* if input path not same as the first part of the shortest db path then this is a bogus request */
        return -ENOENT;
    }

    /* too deep, so need to query result databases */
    if (sqlite3_open(":memory:", &db) == SQLITE_OK) {
        if (att(db) != 0) {
            fprintf(stderr, "Could not attach database files\n");
            sqlite3_close(db);
            return -EIO;
        }

        /* try to find an exact match */
        SNPRINTF(sqlstmt, MAXSQL, "select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from %s where type=='d' and fullpath=='%s'", globalview, path);

        struct StatResults res;
        res.st = stbuf;
        res.count = 0;

        char *err = NULL;
        if (sqlite3_exec(db, sqlstmt, stat_results, &res, &err) != SQLITE_OK) {
            fprintf(stderr, "getattr query for exact match failed: %s\n", err);
        }
        sqlite3_free(err);

        if (res.count == 0) {
            /* try to find the parent and then enter the database */
            char shortpathc[MAXPATH];
            char endpath[MAXPATH];
            SNPRINTF(tpath,MAXXATTR,"%s",path);
            p=&tpath[0]+strlen(tpath)-1;
            if (!strncmp(p,"/",1)) bzero(p,1);
            shortpath(tpath,shortpathc,endpath);

            SNPRINTF(sqlstmt, MAXSQL, "select mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks from %s where fullpath=='%s' and name=='%s'",
                     globalview, shortpathc, endpath);

            if (sqlite3_exec(db, sqlstmt, stat_results, &res, &err) != SQLITE_OK) {
                fprintf(stderr, "getattr query for file match failed: %s\n", err);
            }
            sqlite3_free(err);
        }

        switch (res.count) {
            case 0:
                rc = -ENOENT;
                break;
            case 1:
                rc = 0;
                break;
            default:
                rc = -EBADF;
                break;
        }
    }
    else {
        fprintf(stderr, "getattr error: Could not open in-memory database: %s\n", sqlite3_errmsg(db));
        rc = -EPERM;
    }

    sqlite3_close(db);

    return rc;
}

struct ReaddirResults {
    const char *path;         /* directory provided by fuse */
    void *buf;                /* buffer provided by fuse */
    fuse_fill_dir_t filler;   /* fuse filler function */
    size_t count;             /* how many results have been obtained; should be 1 at the end */
};

static int readdir_results(void *args, int count, char **data, char **columns) {
    struct ReaddirResults *res = (struct ReaddirResults *) args;

    struct stat st;
    fill_stat(&st, count, data);

    char *entry = data[12];
    /* if no name (directory) */
    if (!entry[0]) {
        const size_t path_len     = strlen(res->path);
        const size_t fullpath_len = strlen(data[11]);

        if (path_len != fullpath_len) {
            entry = data[11] + path_len;
            while (*entry && (*entry == '/')) {
                entry++;
            }
        }

    }

    res->filler(res->buf, entry, NULL, 0);
    res->count++;

    return 0;
}

static int gufir_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    int rc = 0;
    sqlite3 *db = NULL;
    char tpath[MAXPATH];
    char sqlstmt[MAXSQL];

    filler(buf, ".", NULL, 0 );
    filler(buf, "..", NULL, 0 );

    /* path is shorter than the mount point path */
    /* assume all directories up to the mount point have the same permissions as the mount point */
    if (strlen(path) < globalmntlen) {
        printf("too short %s\n", path);
        /* find start of entry name */
        char *start = globalmnt + strlen(path);
        while (*start && (*start == '/')) {
            start++;
        }

        /* find end of entry name */
        char * end = start + 1;
        while (*end && (*end != '/')) {
            end++;
        }
        if (*end == '/') {
            end--;
        }

        char entry[MAXPATH];
        memcpy(entry, start, end - start + 1);
        entry[end - start + 1] = '\0';

        filler(buf, entry, &globalst, 0);
        return 0;
    }

    printf("too long %s\n", path);

    /* inside the index, so need to query result databases */
    if (sqlite3_open(":memory:", &db) == SQLITE_OK) {
        if (att(db) != 0) {
            fprintf(stderr, "Could not attach database files\n");
            sqlite3_close(db);
            return -EIO;
        }

        SNPRINTF(sqlstmt, MAXSQL, "SELECT mode,nlink,inode,uid,gid,size,atime,mtime,ctime,blksize,blocks,fullpath,name,type from %s where (pinode == (SELECT inode from %s WHERE type=='d' and fullpath=='%s'))", globalview, globalview, path);

        struct ReaddirResults res;
        res.path = path;
        res.buf = buf;
        res.filler = filler;
        res.count = 0;

        rc = 0;

        /* each result row is pushed into filler */
        char *err = NULL;
        if (sqlite3_exec(db, sqlstmt, readdir_results, &res, &err) != SQLITE_OK) {
            fprintf(stderr, "readdir query failed: %s\n", err);
            rc = -ENOENT;
        }
        sqlite3_free(err);

    }
    else {
        fprintf(stderr, "readdir error: Could not open in-memory database: %s\n", sqlite3_errmsg(db));
        rc = -EPERM;
    }

    sqlite3_close(db);
    return rc;
}

static int gufir_access(const char *path, int mask) {
	return 0;
}

static int gufir_statfs(const char *path, struct statvfs *stbuf) {
        int rc;
        sqlite3_stmt    *res;
        const char      *tail;
        int rec_count;
        char sqlstmt[MAXSQL];
        sqlite3 *mydb;
        int bsize=512;
        long long int totbytes;
        long long int totfiles;

/*
	rc = statvfs("/", stbuf);
	if (rc == -1)
		return -errno;
*/
        rc = sqlite3_open("", &mydb);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "getattr SQL error on open: %s name %s err %s\n",sqlstmt,path,sqlite3_errmsg(mydb));
          return -EIO;
        }
        att(mydb);
        //shortpath(path,shortpathc,endpath);
        totbytes=0;
        totfiles=0;
        SNPRINTF(sqlstmt,MAXSQL,"select sum(size),count(*)from %s where type='f';",globalview);
        rc = sqlite3_prepare_v2(mydb,sqlstmt, MAXSQL, &res, &tail);
        if (rc != SQLITE_OK) {
          fprintf(stderr, "statvfs SQL error on query: %s name %s \n",sqlstmt,path);
          return -1;
        }
        rec_count = 0;

        while (sqlite3_step(res) == SQLITE_ROW) {
          //ncols=sqlite3_column_count(res);
          totbytes=sqlite3_column_int64(res,0);
          totfiles=sqlite3_column_int64(res,1);
          rec_count++;
          break;
        }
        sqlite3_finalize(res);
        closedb(mydb);

        /* these ar available in both OSX and Linux */
        stbuf->f_bsize=bsize;
        stbuf->f_namemax=MAXPATH;
        stbuf->f_fsid=0;
        stbuf->f_frsize=bsize;
        stbuf->f_flag=ST_RDONLY;
        stbuf->f_blocks=(totbytes/bsize)+1;
        stbuf->f_bfree=0;
        stbuf->f_bavail=0;
        stbuf->f_files=totfiles;
        stbuf->f_ffree=0;
        stbuf->f_favail=0;

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
        char sqlstmt[MAXSQL];
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
          SNPRINTF(sqlstmt,MAXSQL,"select xattrs from %s where fullpath='%s' and name='%s';",globalview,shortpathc,endpath);
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
            SNPRINTF(txattr,MAXXATTR,"%s",sqlite3_column_text(res,0));
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
        char sqlstmt[MAXSQL];
        char *p;
        char shortpathc[MAXPATH];
        char endpath[MAXPATH];
        char txattr[MAXXATTR];
        sqlite3 *mydb;

        SNPRINTF(myattr,256,"xattrs");
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
          SNPRINTF(sqlstmt,MAXSQL,"select xattrs from %s where fullpath='%s' and name='%s';",globalview,shortpathc,endpath);
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
            SNPRINTF(txattr,MAXXATTR,"%s",sqlite3_column_text(res,0));
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

struct ReadlinkResults {
    void *buf;                /* buffer provided by fuse */
    size_t size;              /* size of the buffer */
    size_t count;             /* how many results have been obtained; should be 1 at the end */
};

static int readlink_results(void *args, int count, char **data, char **columns) {
    struct ReadlinkResults *res = (struct ReadlinkResults *) args;

    if (count != 1) {
        return -1;
    }

    SNPRINTF(res->buf, res->size, "%s", data[0]);

    return 0;
}

static int gufir_readlink(const char *path, char *buf, size_t size) {
    int rc = 0;
    char sqlstmt[MAXSQL];
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
        return -EPERM;
    }

    shortpath(path,shortpathc,endpath);
    SNPRINTF(sqlstmt,MAXSQL,"select linkname from %s where fullpath='%s' and name='%s';",globalview,shortpathc,endpath);
    rc = sqlite3_open(":memory:", &mydb);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "readlink SQL error on open: %s name %s err %s\n",sqlstmt,path,sqlite3_errmsg(mydb));
        return -EIO;
    }
    att(mydb);

    struct ReadlinkResults res;
    res.buf = buf;
    res.size = size;
    res.count = 0;

    char *err = NULL;
    if (sqlite3_exec(mydb, sqlstmt, readlink_results, &res, &err) != SQLITE_OK) {
        fprintf(stderr, "readlink SQL query error: %s\n", err);
        rc = -EIO;
    }
    sqlite3_free(err);

    return rc;
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
        int i;
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
        SNPRINTF(globaldbname,MAXPATH,"%s",argv[argc-1]);
        argc--;
        // next to next to last arg has to be the table name in the db
        SNPRINTF(globaltab,MAXSQL,"%s",argv[argc-1]);
        argc--;

        SNPRINTF(globalview,MAXSQL,"v%s",globaltab);

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

        SNPRINTF(sqlu,MAXSQL,"select fullpath,length(fullpath) as pl from %s where type=\"d\" order by pl limit 1;",globalview);
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
        SNPRINTF(globalmnt,MAXPATH,"%s",sqlite3_column_text(res,0));
        globalmntlen  = sqlite3_column_int64(res, 1);
        sqlite3_finalize(res);
        closedb(mydb);

        if (strncmp(globalmnt,"/",1)) {
          fprintf(stderr,"path from query resulted in it not starting with a slash %s\n",globalmnt);
          exit(-1);
        }
        if (globalmntlen <1) {
          fprintf(stderr,"path from query resulted in length < 1 %zu %s\n",globalmntlen,globalmnt);
          exit(-1);
        }
        //fprintf(stderr,"path from query resulted in %d %s\n",globalmntlen,globalmnt);

        getcwd(cwd, MAXPATH);
        stat(cwd,&globalst);

	return fuse_main(argc, argv, &gufir_oper, NULL);

}
