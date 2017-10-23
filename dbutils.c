sqlite3 *  opendb(const char *name, char * nameto, struct stat *status, sqlite3 *db, int entriesdb, int createtables)
{
    char *err_msg = 0;
    char dbn[MAXPATH];
    int rc;
    char sqlstmt[1024];
    char *esql = "DROP TABLE IF EXISTS entries;"
          "CREATE TABLE entries(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT);";
//          "CREATE TABLE entries(name TEXT, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT);";
    char *ssql = "DROP TABLE IF EXISTS summary;"
          "CREATE TABLE summary(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT, totfiles INT, totlinks INT, minuid INT, maxuid INT, mingid INT, maxgid INT, minsize INT, maxsize INT, totltk INT, totmtk INT, totltm INT, totmtm INT, totmtg INT, totmtt INT, totsize INT, minctime INT, maxctime INT, minmtime INT, maxmtime INT, minatime INT, maxatime INT, minblocks INT, maxblocks INT, totxattr INT,depth INT);";
    char *tsql = "DROP TABLE IF EXISTS treesummary;"
          "CREATE TABLE treesummary(totsubdirs INT, maxsubdirfiles INT, maxsubdirlinks INT, maxsubdirsize INT, totfiles INT, totlinks INT, minuid INT, maxuid INT, mingid INT, maxgid INT, minsize INT, maxsize INT, totltk INT, totmtk INT, totltm INT, totmtm INT, totmtg INT, totmtt INT, totsize INT, minctime INT, maxctime INT, minmtime INT, maxmtime INT, minatime INT, maxatime INT, minblocks INT, maxblocks INT, totxattr INT,depth INT);";
    if (entriesdb==2) {
      if (createtables) {
        sprintf(dbn,"%s/treesummary.db", name);
      } else {
        sprintf(dbn,"%s/treesummary.db", name);
      }
    } else if(entriesdb==1) {
      if (createtables) {
        sprintf(dbn,"%s/%s/entries.db", nameto,name);
      } else {
        sprintf(dbn,"%s/entries.db", name);
      }
    } else {
      if (createtables) {
        sprintf(dbn,"%s/%s/summary.db", nameto,name);
      } else {
        sprintf(dbn,"%s/summary.db", name);
      }
    }
    rc = sqlite3_open(dbn, &db);
    //printf("rc from open dbn %d %s err %s\n",rc, dbn,sqlite3_errmsg(db));
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Cannot open database: %s %s\n", dbn, sqlite3_errmsg(db));
        sqlite3_close(db);
        return NULL;
    }

    if (createtables) {
      //printf("going to create tables %s\n",dbn);
      if (entriesdb==2) {
         //printf("going to create table treesummary %s\n",dbn);
         rc = sqlite3_exec(db, tsql, 0, 0, &err_msg);
         //fprintf(stderr, "entries SQL error: %s\n", sqlite3_errmsg(db));
      } else if (entriesdb==1) {
         //printf("going to create table entries %s\n",dbn);
         rc = sqlite3_exec(db, esql, 0, 0, &err_msg);
         //fprintf(stderr, "entries SQL error: %s\n", sqlite3_errmsg(db));
      } else {
         //printf("going to create table summary %s\n",dbn);
         rc = sqlite3_exec(db, ssql, 0, 0, &err_msg);
         //fprintf(stderr, "summary SQL error: %s\n", sqlite3_errmsg(db));
      }
      if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return NULL;
      }
    }

    sqlite3_free(err_msg);
    //printf("returning from opendb ok\n");
    return db;
}

int querydb(const char *name, struct stat *st, sqlite3 *db, char *type, char *linkname, int xattrs, char * xattr,char *sqlstmt, int *recs, int printdir, int issummary,int pinodeplace, long long dfpinode, int printpath)
{
     sqlite3_stmt    *res;
     int     error = 0;
     int     rec_count = 0;
     const char      *errMSG;
     const char      *tail;
     int ncols;
     int cnt;
     char prefix[MAXPATH];
     char *pp;
     int i;

     error = sqlite3_prepare_v2(db, sqlstmt, 1000, &res, &tail);

     if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s name %s errr %s\n",sqlstmt,name,sqlite3_errmsg(db));
          return 0;
     }
     //printf("printdir %d issummary %d\n",printdir,issummary);
     //ncols=sqlite3_column_count(res);
     //printf("query ncols %d\n",ncols);
     //printf("in querydb pinodeplace %d dfpinode %lld\n",pinodeplace,dfpinode); 
     if (issummary) {
       while (sqlite3_step(res) == SQLITE_ROW) {
         if (printdir) {
           if (!strncmp(type,"d",1)) {
              sprintf(prefix,"%s",name);
              i=strlen(prefix);
              pp=prefix+i;
              //printf("cutting name down %s len %d\n",prefix,i);
              while (i > 0) {
                if (!strncmp(pp,"/",1)) {
                   bzero(pp,1);
                   break;            
                } 
                pp--;
                i--;
              }
              //printf("cutting name down afte %s\n",prefix);
              if (printpath) printf("%s/", prefix);
           } else {
              if (printpath) printf("%s|",name);
           }
           ncols=sqlite3_column_count(res);
           cnt=0;
           while (ncols > 0) {
             if (pinodeplace) {
               if (cnt == (pinodeplace-1)) {
                 printf("%lld|",dfpinode);
               }
             }
             printf("%s|", sqlite3_column_text(res, cnt));
             //printf("%s|", sqlite3_column_text(res, 0));
             //printf("%s|", sqlite3_column_text(res, 1));
             //printf("%s|", sqlite3_column_text(res, 2));
             //printf("%u\n", sqlite3_column_int(res, 3));
             ncols--; cnt++;
           }
           printf("\n");
         }
         rec_count++;
       }
     } else {
         while (sqlite3_step(res) == SQLITE_ROW) {
           if (printpath) printf("%s/", name);
           cnt=0;
           ncols=sqlite3_column_count(res);
           while (ncols > 0) {
             if (pinodeplace) {
               if (cnt == (pinodeplace-1)) {
                 //printf("pinodeplace %d pinode %lld\n",pinodeplace,dfpinode);
                 printf("%lld|",dfpinode);
               }
             }
             printf("%s|", sqlite3_column_text(res,cnt));
             //printf("%s|", sqlite3_column_text(res, 0));
             //printf("%s|", sqlite3_column_text(res, 1));
             //printf("%s|", sqlite3_column_text(res, 2));
             //printf("%u\n", sqlite3_column_int(res, 3));
             ncols--; cnt++;
           }
           printf("\n");

           rec_count++;
         }

     }

    //printf("We received %d records.\n", rec_count);
    *recs=rec_count;

    sqlite3_finalize(res);
    return 0;
}

int querytsdb(const char *name, struct sum *sumin, sqlite3 *db, int *recs,int ts)
{
     sqlite3_stmt    *res;
     int     error = 0;
     int     rec_count = 0;
     const char      *errMSG;
     const char      *tail;
     int ncols;
     int cnt;
     char prefix[MAXPATH];
     char *pp;
     int i;
     char sqlstmt[MAXSQL];
     //struct sum sumin;

     if (!ts) {
       sprintf(sqlstmt,"select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr from summary;");
     } else {
       sprintf(sqlstmt,"select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr,totsubdirs,maxsubdirfiles,maxsubdirlinks,maxsubdirsize from treesummary;");
     }
     error = sqlite3_prepare_v2(db, sqlstmt, 1000, &res, &tail);

     if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s name %s errr %s\n",sqlstmt,name,sqlite3_errmsg(db));
          return 0;
     }
     //ncols=sqlite3_column_count(res);
     //printf("tsdb %s\n",name);
     // while (sqlite3_step(res) == SQLITE_ROW) {
     sqlite3_step(res);
     //ncols=sqlite3_column_count(res);
     sumin->totfiles=atoll((const char *)sqlite3_column_text(res, 0));
     sumin->totlinks=atoll((const char *)sqlite3_column_text(res, 1));
     sumin->minuid=atoll((const char *)sqlite3_column_text(res, 2));
     sumin->maxuid=atoll((const char *)sqlite3_column_text(res, 3));
     sumin->mingid=atoll((const char *)sqlite3_column_text(res, 4));
     sumin->maxgid=atoll((const char *)sqlite3_column_text(res, 5));
     sumin->minsize=atoll((const char *)sqlite3_column_text(res, 6));
     sumin->maxsize=atoll((const char *)sqlite3_column_text(res, 7));
     sumin->totltk=atoll((const char *)sqlite3_column_text(res, 8));
     sumin->totmtk=atoll((const char *)sqlite3_column_text(res, 9));
     sumin->totltm=atoll((const char *)sqlite3_column_text(res, 10));
     sumin->totmtm=atoll((const char *)sqlite3_column_text(res, 11));
     sumin->totmtg=atoll((const char *)sqlite3_column_text(res, 12));
     sumin->totmtt=atoll((const char *)sqlite3_column_text(res, 13));
     sumin->totsize=atoll((const char *)sqlite3_column_text(res,14));
     sumin->minctime=atoll((const char *)sqlite3_column_text(res, 15));
     sumin->maxctime=atoll((const char *)sqlite3_column_text(res, 16));
     sumin->minmtime=atoll((const char *)sqlite3_column_text(res, 17));
     sumin->maxmtime=atoll((const char *)sqlite3_column_text(res, 18));
     sumin->minatime=atoll((const char *)sqlite3_column_text(res, 19));
     sumin->maxatime=atoll((const char *)sqlite3_column_text(res, 20));
     sumin->minblocks=atoll((const char *)sqlite3_column_text(res, 21));
     sumin->maxblocks=atoll((const char *)sqlite3_column_text(res, 22));
     sumin->totxattr=atoll((const char *)sqlite3_column_text(res, 23));
     if (ts) {
       sumin->totsubdirs=atoll((const char *)sqlite3_column_text(res, 24));
       sumin->maxsubdirfiles=atoll((const char *)sqlite3_column_text(res, 25));
       sumin->maxsubdirlinks=atoll((const char *)sqlite3_column_text(res, 26));
       sumin->maxsubdirsize=atoll((const char *)sqlite3_column_text(res, 27));
     }
     //printf("tsdb: totfiles %d totlinks %d minuid %d\n",sumin->totfiles,sumin->totlinks,sumin->minuid);
     //cnt=0;
     //printf("%s|", sqlite3_column_text(res, cnt));
     //ncols--; cnt++;
     //printf("\n");
     //rec_count++;
     // }

    //printf("We received %d records.\n", rec_count);
    //*recs=rec_count;

    sqlite3_finalize(res);
    return 0;
}

int startdb(sqlite3 *db)
{
    char *err_msg = 0;
    int rc;
    rc = sqlite3_exec(db, "BEGIN TRANSACTION", NULL , NULL, &err_msg);
    if (rc != SQLITE_OK) printf("begin transaction issue %s\n",sqlite3_errmsg(db));
    sqlite3_free(err_msg);
    return 0;
}

int stopdb(sqlite3 *db)
{
    char *err_msg = 0;
    int rc;
    sqlite3_exec(db,"END TRANSACTION",NULL, NULL, &err_msg);
    sqlite3_free(err_msg);
    return 0;
}

int closedb(sqlite3 *db)
{
    char *err_msg = 0;
    int rc;
    sqlite3_free(err_msg);
    sqlite3_close(db);
    return 0;
}

int insertdbfin(sqlite3 *db,sqlite3_stmt *res)
{
    char *err_msg = 0;
    int rc;
    
    sqlite3_finalize(res);

    return 0;
}

sqlite3_stmt * insertdbprep(sqlite3 *db,sqlite3_stmt *res)
{
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    const char *tail;
    int error;
    sqlite3_stmt *reso;

    /*******  this is a big dangerous as all the records need to be written to the db and some time for the os to write to disk  ****/
    error=sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, &err_msg);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep setting sync off: error %d err %s\n",error,sqlite3_errmsg(db));
    }
//"CREATE TABLE entries(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname text, xattrstext);
    sprintf(sqlstmt,"INSERT INTO entries VALUES (@name,@type,@inode,@mode,@nlink,@uid,@gid,@size,@blksize,@blocks,@atime,@mtime, @ctime,@linkname,@xattrs)");
    //printf(" prep INSERT INTO entries VALUES (@name,&type,&inode,&mode,&nlink,&uid,&gid,&size,&blksize,&blocks,&atime,&mtime, &ctime,&linkname,&xattrs)\n");
    error= sqlite3_prepare_v2(db,  sqlstmt, MAXSQL, &reso, &tail);
    //printf("in prep reso %d\n",reso);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep: error %d %s err %s\n",error,sqlstmt,sqlite3_errmsg(db));
          return 0;
    }
    return reso;
}

int insertdbgo(const char *name, struct stat *st, sqlite3 *db,char *type, char *linkname, int xattrs, char *xattr, int formatxattr, sqlite3_stmt *res)
{
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    const char *tail;
    int error;
    char bufo[MAXXATTR];
    char *bufop;
    char *xattrp;
    char *zname;
    char *ztype;
    char *zlinkname;
    char *zbufo;
    int cnt;

    //printf("dbgo res %d\n",res);
//    sprintf(sqlstmt,"INSERT INTO entries VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\');",zname,ztype,st->st_ino,st->st_mode,st->st_nlink,st->st_uid,st->st_gid,st->st_size,st->st_blksize,st->st_blocks,st->st_atime,st->st_mtime,st->st_ctime,zlinkname,zbufo);
    cnt = 0;
    xattrp=xattr;
    bufop=bufo;
    bzero(bufo,sizeof(bufo));
    //printf("insertdbgo xattrs %d\n",xattrs);
    if (xattrs > 0) {
      if (formatxattr) {
        while (cnt < xattrs) {
//char cat[] = "\x1F";
//          strncpy(bufop,xattrdelim,1); bufop=bufop+1;
//          strcpy(bufop,"<"); bufop=bufop+1; 
          strcpy(bufop,xattrp); bufop=bufop+strlen(xattrp); xattrp=xattrp+strlen(xattrp)+1;
          strncpy(bufop,xattrdelim,1); bufop=bufop+1;
//          strcpy(bufop,">"); bufop=bufop+1;
//          strncpy(bufop,xattrdelim,1); bufop=bufop+1;
          strcpy(bufop,xattrp); bufop=bufop+strlen(xattrp); xattrp=xattrp+strlen(xattrp)+1;
          strncpy(bufop,xattrdelim,1); bufop=bufop+1;
          cnt++;
        }
      } else {
        sprintf(bufo,"%s",xattr);
      }
    }
    zname = sqlite3_mprintf("%q",name);
    ztype = sqlite3_mprintf("%q",type);
    zlinkname = sqlite3_mprintf("%q",linkname);
    zbufo = sqlite3_mprintf("%q",bufo);
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgo bind name: %s error %d err %s\n",name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(res,3,st->st_ino);
    sqlite3_bind_int64(res,4,st->st_mode);
    sqlite3_bind_int64(res,5,st->st_nlink);
    sqlite3_bind_int64(res,6,st->st_uid);
    sqlite3_bind_int64(res,7,st->st_gid);
    sqlite3_bind_int64(res,8,st->st_size);
    sqlite3_bind_int64(res,9,st->st_blksize);
    sqlite3_bind_int64(res,10,st->st_blocks);
    sqlite3_bind_int64(res,11,st->st_atime);
    sqlite3_bind_int64(res,12,st->st_mtime);
    sqlite3_bind_int64(res,13,st->st_ctime);
    if (error != SQLITE_OK) printf("insertdbgo bind ctime: error %d %s %s %ld %s\n",error,name,type,st->st_ctime,sqlite3_errmsg(db));
    sqlite3_bind_text(res,14,zlinkname,-1,SQLITE_TRANSIENT);
    error=sqlite3_bind_text(res,15,zbufo,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) printf("insertdbgo bind xattr: error %d %s %s %s %s\n",error,name,type,bufo,sqlite3_errmsg(db));
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zbufo);
    error = sqlite3_step(res);
    if (error != SQLITE_ROW) {
          //fprintf(stderr, "SQL error on insertdbgo: error %d err %s\n",error,sqlite3_errmsg(db));
          //return 0;
    }
    sqlite3_clear_bindings(res);
    sqlite3_reset(res); 

    return 0;
}

int insertdb(const char *name, struct stat *st, sqlite3 *db, char *type, char *linkname, int xattrs, char * xattr, char *records,int recsize,int maxrecs)
{
    char buf[MAXXATTR];
    char bufv[MAXXATTR];
    char bufo[MAXXATTR];
    char *bufop;
    char *xattrp;
    int cnt;
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int len;
    int rlen;

    cnt = 0;
    if (maxrecs==0) {
      rc = sqlite3_exec(db, records, 0, 0, &err_msg);
      //fprintf(stderr, "SQL flush on insert: %s sqlcmd %s\n", sqlite3_errmsg(db),records);
      if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert: %s sqlcmd %s\n", err_msg,records);
        sqlite3_free(err_msg);
        return -1;
        //caller should really check this 
      }
      sqlite3_free(err_msg);
      return 0;
    }

    xattrp=xattr;
    bufop=bufo;
    bzero(bufo,sizeof(bufo));
    if (xattrs > 0) {
      while (cnt < xattrs) {
        strcpy(bufop,"<"); bufop=bufop+1; 
        strcpy(bufop,xattrp); bufop=bufop+strlen(xattrp); xattrp=xattrp+strlen(xattrp)+1; strcpy(bufop,">"); bufop=bufop+1;
        strcpy(bufop,xattrp); bufop=bufop+strlen(xattrp); xattrp=xattrp+strlen(xattrp)+1;
        cnt++;
      }
      //printf("in insertdb %s\n",bufo);
    }
    char *zname = sqlite3_mprintf("%q",name);
    char *ztype = sqlite3_mprintf("%q",type);
    char *zlinkname = sqlite3_mprintf("%q",linkname);
    char *zbufo = sqlite3_mprintf("%q",bufo);
    sprintf(sqlstmt,"INSERT INTO entries VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\');",zname,ztype,st->st_ino,st->st_mode,st->st_nlink,st->st_uid,st->st_gid,st->st_size,st->st_blksize,st->st_blocks,st->st_atime,st->st_mtime,st->st_ctime,zlinkname,zbufo);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zbufo);
    len=strlen(sqlstmt)+1;
    rlen=strlen(sqlstmt);
    if ((recsize+rlen) >= maxrecs) {
      //rc = sqlite3_exec(db, sqlstmt, 0, 0, &err_msg);
      rc = sqlite3_exec(db, records, 0, 0, &err_msg);
      //fprintf(stderr, "SQL error on insert: %s sqlcmd %s\n", sqlite3_errmsg(db),sqlstmt);
      if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert: %s sqlcmd %s\n", err_msg,sqlstmt);
        sqlite3_free(err_msg);
        return -1;
        //caller should really check this 
      }
      sqlite3_free(err_msg);
      return 0;
    } else {
      strcat(records,sqlstmt);
      //printf("insertdb records=%s\n",records);
      return rlen;
    }
    return -1;
}

int insertsumdb(const char *name, struct stat *st, sqlite3 *sdb, char *type, char *linkname, int xattrs, char * xattr, struct sum *su,int pinode)
{
    char buf[MAXXATTR];
    char bufv[MAXXATTR];
    char bufo[MAXXATTR];
    char *bufop;
    char *xattrp;
    int cnt;
    int found;
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int len=0;;
    const char *shortname;
    int depth;
    int i;

    /*???????? need to check strings name, linkname, and xattr for special characters taht will trip up sql parsing */
/*
    xattrp=xattr;
    bufop=bufo;
    bzero(bufo,sizeof(bufo));
    if (xattrs > 0) {
      while (cnt < xattrs) {
        strcpy(bufop,"<"); bufop=bufop+1; 
        strcpy(bufop,xattrp); bufop=bufop+strlen(xattrp); xattrp=xattrp+strlen(xattrp)+1; strcpy(bufop,">"); bufop=bufop+1;
        strcpy(bufop,xattrp); bufop=bufop+strlen(xattrp); xattrp=xattrp+strlen(xattrp)+1;
        cnt++;
      }
      //printf("in insertdb %s\n",bufo);
    }
*/
    sprintf(bufo,"%s",xattr);
    depth=0;
    i=0;
    while (i < strlen(name)) {
      if (!strncmp(name+i,"/",1)) depth++;
      i++;
    } 
    //printf("dbutil insertsum name %s depth %d\n",name,depth);
    shortname=name;
    len=strlen(name);
    cnt=0;
    found=0;
    while (len > 0) {
       if (!memcmp(shortname,"/",1)) {
          found=cnt;
       } 
       cnt++;
       len--;
       shortname++;
    }
    if (found > 0) {
      shortname=name+found+1;
    } else {
      shortname=name;
    }
    /*???????? obviously there would be more fields */
    char *zname = sqlite3_mprintf("%q",shortname);
    char *ztype = sqlite3_mprintf("%q",type);
    char *zlinkname = sqlite3_mprintf("%q",linkname);
    char *zbufo = sqlite3_mprintf("%q",bufo);
    sprintf(sqlstmt,"INSERT INTO summary VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\', %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d);",
       zname, ztype, st->st_ino,st->st_mode,st->st_nlink,st->st_uid,st->st_gid,st->st_size,st->st_blksize,st->st_blocks,st->st_atime,st->st_mtime,st->st_ctime,zlinkname,zbufo,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zbufo);
    //printf("INSERT INTO summary VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\', %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d);",
    //   zname, ztype, st->st_ino,st->st_mode,st->st_nlink,st->st_uid,st->st_gid,st->st_size,st->st_blksize,st->st_blocks,st->st_atime,st->st_mtime,st->st_ctime,zlinkname,zbufo,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->mtg, su->mtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth);
    //printf("INSERT INTO summary VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\', %d, %d);",zname,ztype,st->st_ino,st->st_mode,st->st_nlink,st->st_uid,st->st_gid,st->st_size,st->st_blksize,st->st_blocks,st->st_atime,st->st_mtime,st->st_ctime,zlinkname,zbufo,summary->totfiles, summary->totsize);
    len=strlen(sqlstmt)+1;

    rc = sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg);

    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }
    sqlite3_free(err_msg);
    return 0;
}

int inserttreesumdb(const char *name, struct stat *st, sqlite3 *sdb, char *type, char *linkname, int xattrs, char * xattr, struct sum *su,int pinode)
{
    int cnt;
    int found;
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int len=0;;
    int depth;
    int i;

    depth=0;
    i=0;
    while (i < strlen(name)) {
      if (!strncmp(name+i,"/",1)) depth++;
      i++;
    } 
    sprintf(sqlstmt,"INSERT INTO treesummary VALUES (%lld, %lld, %lld, %lld,%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d);",
       su->totsubdirs, su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth);
    len=strlen(sqlstmt)+1;

    rc = sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg);
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }
    sqlite3_free(err_msg);
    return 0;
}

