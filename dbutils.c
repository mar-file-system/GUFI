sqlite3 *  attachdb(const char *name, sqlite3 *db, char *dbn)
{
  char *err_msg = 0;
  int rc;
  char sqlat[MAXSQL];

  sprintf(sqlat,"ATTACH \'%s/db.db\' as %s",name,dbn);
  rc=sqlite3_exec(db, sqlat,0, 0, &err_msg);
  if (rc != SQLITE_OK) {
      fprintf(stderr, "Cannot attach database: %s/db.db %s\n", name, sqlite3_errmsg(db));
      sqlite3_close(db);
      return NULL;
  }
  sqlite3_free(err_msg);
 
  return 0;
}

sqlite3 *  detachdb(const char *name, sqlite3 *db, char *dbn)
{
  char *err_msg = 0;
  int rc;
  char sqldet[MAXSQL];

  sprintf(sqldet,"DETACH %s",dbn);
  rc=sqlite3_exec(db, sqldet,0, 0, &err_msg);
  if (rc != SQLITE_OK) {
      fprintf(stderr, "Cannot detach database: %s/db.db %s\n", name, sqlite3_errmsg(db));
      sqlite3_close(db);
      return NULL;
  }
  sqlite3_free(err_msg);
 
  return 0;
}

sqlite3 *  opendb(const char *name, sqlite3 *db, int openwhat, int createtables)
{
    char *err_msg = 0;
    char dbn[MAXPATH];
    int rc;

    sprintf(dbn,"%s/db.db", name);
    if (createtables>0) {
        if (openwhat != 3)
          sprintf(dbn,"%s/%s/db.db", in.nameto,name);
    }

    if (createtables==0) {
        if (openwhat == 6)
          sprintf(dbn,"%s/%s/db.db", in.nameto,name);
    }

    if (createtables==0) {
      if (openwhat==5) {
        sprintf(dbn,"%s", name);
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
      if (openwhat==1 || openwhat==4)
         rc = sqlite3_exec(db, esql, 0, 0, &err_msg);
      if (openwhat==3) {
         rc = sqlite3_exec(db, tsql, 0, 0, &err_msg);
         rc = sqlite3_exec(db, vtssqldir, 0, 0, &err_msg);
         rc = sqlite3_exec(db, vtssqluser, 0, 0, &err_msg);
      }
      if (openwhat==2 || openwhat==4) {
         rc = sqlite3_exec(db, ssql, 0, 0, &err_msg);
         rc = sqlite3_exec(db, vssqldir, 0, 0, &err_msg);
         rc = sqlite3_exec(db, vssqluser, 0, 0, &err_msg);
         rc = sqlite3_exec(db, vesql, 0, 0, &err_msg);
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

int rawquerydb(const char *name, int isdir, sqlite3 *db, char *sqlstmt,int printpath, int printheader, int printing, int ptid)
{
     sqlite3_stmt    *res;
     int     error = 0;
     int     rec_count = 0;
     const char      *errMSG;
     const char      *tail;
     int ncols;
     int cnt;
     int onetime;
     char prefix[MAXPATH];
     char shortname[MAXPATH];
     char *pp;
     int i;
     FILE * out;
     char  ffielddelim[2];
 
     out = stdout;
     if (in.outfile > 0) out = gts.outfd[ptid];

     if (in.dodelim == 0) {
       sprintf(ffielddelim,"|");
     }
     if (in.dodelim == 1) {
       sprintf(ffielddelim,"%s",fielddelim);
     }
     if (in.dodelim == 2) {
       sprintf(ffielddelim,"%s",in.delim);
     }

     sprintf(shortname,"%s",name);
     if (isdir > 0) {
          i=0;
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
          sprintf(shortname,"%s",prefix);
     }
     
     error = sqlite3_prepare_v2(db, sqlstmt, 1000, &res, &tail);

     if (error != SQLITE_OK) {
          // should put this into an error we pass back
          //fprintf(stderr, "SQL error on query: %s name %s errr %s\n",sqlstmt,name,sqlite3_errmsg(db));
          return -1;
     }
     //printf("running on %s query %s printpath %d\n",name,sqlstmt,printpath);
     onetime=0;
     while (sqlite3_step(res) == SQLITE_ROW) {
      if (printing) {
       if (printheader) {
         if (onetime == 0) {
           cnt=0;
           ncols=sqlite3_column_count(res);
           while (ncols > 0) {
             if (cnt==0) {
               if (printpath) fprintf(out,"path/%s",ffielddelim);
             }
             fprintf(out,"%s%s", sqlite3_column_name(res,cnt),ffielddelim);
             //fprintf(out,"%s%s", sqlite3_column_decltype(res,cnt),ffielddelim);
             ncols--; cnt++;
           }
           fprintf(out,"\n");
           onetime++;
         }
       }
       //if (printpath) printf("%s/", name);
       cnt=0;
       ncols=sqlite3_column_count(res);
       while (ncols > 0) {
         if (cnt==0) {
           if (printpath) fprintf(out,"%s/%s",shortname,ffielddelim);
         }
         fprintf(out,"%s%s", sqlite3_column_text(res,cnt),ffielddelim);
         ncols--; cnt++;
       }
       fprintf(out,"\n");
      }

      rec_count++;
     }

    //printf("We received %d records.\n", rec_count);

    sqlite3_finalize(res);
    return(rec_count);
}

int querytsdb(const char *name, struct sum *sumin, sqlite3 *db, int *recs,int ts)
{
     sqlite3_stmt    *res;
     int     error = 0;
     const char      *errMSG;
     const char      *tail;
     char sqlstmt[MAXSQL];
     //struct sum sumin;

     if (!ts) {
       sprintf(sqlstmt,"select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr,mincrtime,maxcrtime,minossint1,maxossint1,totossint1,minossint2,maxossint2,totossint2,minossint3,maxossint3,totossint3,minossint4,maxossint4,totossint4 from summary where rectype=0;;");
     } else {
       sprintf(sqlstmt,"select totfiles,totlinks,minuid,maxuid,mingid,maxgid,minsize,maxsize,totltk,totmtk,totltm,totmtm,totmtg,totmtt,totsize,minctime,maxctime,minmtime,maxmtime,minatime,maxatime,minblocks,maxblocks,totxattr,mincrtime,maxcrtime,minossint1,maxossint1,totossint1,minossint2,maxossint2,totossint2,minossint3,maxossint3,totossint3,minossint4,maxossint4,totossint4,totsubdirs,maxsubdirfiles,maxsubdirlinks,maxsubdirsize from treesummary where rectype=0;");
     }
     error = sqlite3_prepare_v2(db, sqlstmt, 1000, &res, &tail);

     if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on query: %s name %s errr %s\n",sqlstmt,name,sqlite3_errmsg(db));
          return 0;
     }
     sqlite3_step(res);
     //sumin->totfiles=atoll((const char *)sqlite3_column_text(res, 0));
     sumin->totfiles=sqlite3_column_int64(res, 0);
     sumin->totlinks=sqlite3_column_int64(res, 1);
     sumin->minuid=sqlite3_column_int64(res, 2);
     sumin->maxuid=sqlite3_column_int64(res, 3);
     sumin->mingid=sqlite3_column_int64(res, 4);
     sumin->maxgid=sqlite3_column_int64(res, 5);
     sumin->minsize=sqlite3_column_int64(res, 6);
     sumin->maxsize=sqlite3_column_int64(res, 7);
     sumin->totltk=sqlite3_column_int64(res, 8);
     sumin->totmtk=sqlite3_column_int64(res, 9);
     sumin->totltm=sqlite3_column_int64(res, 10);
     sumin->totmtm=sqlite3_column_int64(res, 11);
     sumin->totmtg=sqlite3_column_int64(res, 12);
     sumin->totmtt=sqlite3_column_int64(res, 13);
     sumin->totsize=sqlite3_column_int64(res,14);
     sumin->minctime=sqlite3_column_int64(res, 15);
     sumin->maxctime=sqlite3_column_int64(res, 16);
     sumin->minmtime=sqlite3_column_int64(res, 17);
     sumin->maxmtime=sqlite3_column_int64(res, 18);
     sumin->minatime=sqlite3_column_int64(res, 19);
     sumin->maxatime=sqlite3_column_int64(res, 20);
     sumin->minblocks=sqlite3_column_int64(res, 21);
     sumin->maxblocks=sqlite3_column_int64(res, 22);
     sumin->totxattr=sqlite3_column_int64(res, 23);

     sumin->mincrtime=sqlite3_column_int64(res, 24);
     sumin->maxcrtime=sqlite3_column_int64(res, 25);
     sumin->minossint1=sqlite3_column_int64(res, 26);
     sumin->maxossint1=sqlite3_column_int64(res, 27);
     sumin->totossint1=sqlite3_column_int64(res, 28);
     sumin->minossint2=sqlite3_column_int64(res, 29);
     sumin->maxossint2=sqlite3_column_int64(res, 30);
     sumin->totossint2=sqlite3_column_int64(res, 31);
     sumin->minossint3=sqlite3_column_int64(res, 32);
     sumin->maxossint3=sqlite3_column_int64(res, 33);
     sumin->totossint3=sqlite3_column_int64(res, 34);
     sumin->minossint4=sqlite3_column_int64(res, 35);
     sumin->maxossint4=sqlite3_column_int64(res, 36);
     sumin->totossint4=sqlite3_column_int64(res, 37);

     if (ts) {
       sumin->totsubdirs=sqlite3_column_int64(res, 38);
       sumin->maxsubdirfiles=sqlite3_column_int64(res,39 );
       sumin->maxsubdirlinks=sqlite3_column_int64(res,40 );
       sumin->maxsubdirsize=sqlite3_column_int64(res,41 );
     }

     //printf("tsdb: totfiles %d totlinks %d minuid %d\n",sumin->totfiles,sumin->totlinks,sumin->minuid);
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
    //sprintf(sqlstmt,"INSERT INTO entries VALUES (@name,@type,@inode,@mode,@nlink,@uid,@gid,@size,@blksize,@blocks,@atime,@mtime, @ctime,@linkname,@xattrs)");
    sprintf(sqlstmt,"%s",esqli);
    //printf(" prep INSERT INTO entries VALUES (@name,&type,&inode,&mode,&nlink,&uid,&gid,&size,&blksize,&blocks,&atime,&mtime, &ctime,&linkname,&xattrs)\n");
    error= sqlite3_prepare_v2(db,  sqlstmt, MAXSQL, &reso, &tail);
    //printf("in prep reso %d\n",reso);
    if (error != SQLITE_OK) {
          fprintf(stderr, "SQL error on insertdbprep: error %d %s err %s\n",error,sqlstmt,sqlite3_errmsg(db));
          return 0;
    }
    return reso;
}

int insertdbgo(struct work *pwork, sqlite3 *db, sqlite3_stmt *res)
{
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int error;
    char *zname;
    char *ztype;
    char *zlinkname;
    char *zxattr;
    char *zosstext1;
    char *zosstext2;
    int len=0;;
    const char *shortname;
    int i;
    int found;
    int cnt;


//    sprintf(sqlstmt,"INSERT INTO entries VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\');",zname,ztype,st->st_ino,st->st_mode,st->st_nlink,st->st_uid,st->st_gid,st->st_size,st->st_blksize,st->st_blocks,st->st_atime,st->st_mtime,st->st_ctime,zlinkname,zbufo);
/*
CREATE TABLE entries(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT, crtime INT, ossint1 INT, ossint2 INT, ossint3 INT, ossint4 INT, osstext1 TEXT, osstext2 TEXT);";
char *esqli = "INSERT INTO entries VALUES (@name,@type,@inode,@mode,@nlink,@uid,@gid,@size,@blksize,@blocks,@atime,@mtime, @ctime,@linkname,@xattrs,@crtime,@ossint1,@ossint2,@ossint3,@ossint4,@osstext1,@osstext2);";
*/
    shortname=pwork->name;
    len=strlen(pwork->name);
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
      shortname=pwork->name+found+1;
    } else {
      shortname=pwork->name;
    }

    zname = sqlite3_mprintf("%q",shortname);
    ztype = sqlite3_mprintf("%q",pwork->type);
    zlinkname = sqlite3_mprintf("%q",pwork->linkname);
    zxattr = sqlite3_mprintf("%q",pwork->xattr);
    zosstext1 = sqlite3_mprintf("%q",pwork->osstext1);
    zosstext2 = sqlite3_mprintf("%q",pwork->osstext2);
    error=sqlite3_bind_text(res,1,zname,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) fprintf(stderr, "SQL insertdbgo bind name: %s error %d err %s\n",pwork->name,error,sqlite3_errmsg(db));
    sqlite3_bind_text(res,2,ztype,-1,SQLITE_TRANSIENT);
    sqlite3_bind_int64(res,3,pwork->statuso.st_ino);
    sqlite3_bind_int64(res,4,pwork->statuso.st_mode);
    sqlite3_bind_int64(res,5,pwork->statuso.st_nlink);
    sqlite3_bind_int64(res,6,pwork->statuso.st_uid);
    sqlite3_bind_int64(res,7,pwork->statuso.st_gid);
    sqlite3_bind_int64(res,8,pwork->statuso.st_size);
    sqlite3_bind_int64(res,9,pwork->statuso.st_blksize);
    sqlite3_bind_int64(res,10,pwork->statuso.st_blocks);
    sqlite3_bind_int64(res,11,pwork->statuso.st_atime);
    sqlite3_bind_int64(res,12,pwork->statuso.st_mtime);
    sqlite3_bind_int64(res,13,pwork->statuso.st_ctime);
    sqlite3_bind_text(res,14,zlinkname,-1,SQLITE_TRANSIENT);
    error=sqlite3_bind_text(res,15,zxattr,-1,SQLITE_TRANSIENT);
    if (error != SQLITE_OK) printf("insertdbgo bind xattr: error %d %s %s %s %s\n",error,pwork->name,pwork->type,pwork->xattr,sqlite3_errmsg(db));
    sqlite3_bind_int64(res,16,pwork->crtime);
    sqlite3_bind_int64(res,17,pwork->ossint1);
    sqlite3_bind_int64(res,18,pwork->ossint2);
    sqlite3_bind_int64(res,19,pwork->ossint3);
    sqlite3_bind_int64(res,20,pwork->ossint4);
    error=sqlite3_bind_text(res,21,zosstext1,-1,SQLITE_TRANSIENT);
    error=sqlite3_bind_text(res,22,zosstext1,-1,SQLITE_TRANSIENT);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zxattr);
    sqlite3_free(zosstext1);
    sqlite3_free(zosstext2);
    error = sqlite3_step(res);
    if (error != SQLITE_ROW) {
          //fprintf(stderr, "SQL error on insertdbgo: error %d err %s\n",error,sqlite3_errmsg(db));
          //return 0;
    }
    sqlite3_clear_bindings(res);
    sqlite3_reset(res); 

    return 0;
}

int insertsumdb(sqlite3 *sdb, struct work *pwork,struct sum *su)
{
    int cnt;
    int found;
    char *err_msg = 0;
    char sqlstmt[MAXSQL];
    int rc;
    int len=0;;
    const char *shortname;
    int depth;
    int i;
    int rectype;

    rectype=0; // directory summary record type
    depth=0;
    i=0;
    while (i < strlen(pwork->name)) {
      if (!strncmp(pwork->name+i,"/",1)) depth++;
      i++;
    } 
    //printf("dbutil insertsum name %s depth %d\n",pwork->name,depth);
    shortname=pwork->name;
    len=strlen(pwork->name);
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
      shortname=pwork->name+found+1;
    } else {
      shortname=pwork->name;
    }
    if (strlen(shortname) < 1) printf("***** shortname is < 1 %s\n",pwork->name);

/*
CREATE TABLE summary(name TEXT PRIMARY KEY, type TEXT, inode INT, mode INT, nlink INT, uid INT, gid INT, size INT, blksize INT, blocks INT, atime INT, mtime INT, ctime INT, linkname TEXT, xattrs TEXT, totfiles INT, totlinks INT, minuid INT, maxuid INT, mingid INT, maxgid INT, minsize INT, maxsize INT, totltk INT, totmtk INT, totltm INT, totmtm INT, totmtg INT, totmtt INT, totsize INT, minctime INT, maxctime INT, minmtime INT, maxmtime INT, minatime INT, maxatime INT, minblocks INT, maxblocks INT, totxattr INT,depth INT, mincrtime INT, maxcrtime INT, minossint1 INT, maxossint1 INT, totossint1 INT, minossint2 INT, maxossint2, totossint2 INT, minossint3 INT, maxossint3, totossint3 INT,minossint4 INT, maxossint4 INT, totossint4 INT, rectype INT, pinode INT);
*/
    char *zname = sqlite3_mprintf("%q",shortname);
    char *ztype = sqlite3_mprintf("%q",pwork->type);
    char *zlinkname = sqlite3_mprintf("%q",pwork->linkname);
    char *zxattr = sqlite3_mprintf("%q",pwork->xattr);
    sprintf(sqlstmt,"INSERT INTO summary VALUES (\'%s\', \'%s\',%lld, %d, %d, %d, %d, %lld, %d, %lld, %ld, %ld, %ld, \'%s\', \'%s\', %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld);",
       zname, ztype, pwork->statuso.st_ino,pwork->statuso.st_mode,pwork->statuso.st_nlink,pwork->statuso.st_uid,pwork->statuso.st_gid,pwork->statuso.st_size,pwork->statuso.st_blksize,pwork->statuso.st_blocks,pwork->statuso.st_atime,pwork->statuso.st_mtime,pwork->statuso.st_ctime,zlinkname,zxattr,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth, su->mincrtime, su->maxcrtime, su->minossint1, su->maxossint1, su->totossint1, su->minossint2, su->maxossint2, su->totossint2, su->minossint3, su->maxossint3, su->totossint3, su->minossint4,su->maxossint4, su->totossint4, rectype, pwork->pinode);
    sqlite3_free(zname);
    sqlite3_free(ztype);
    sqlite3_free(zlinkname);
    sqlite3_free(zxattr);

    rc = sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg);

    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }
    sqlite3_free(err_msg);
    return 0;
}

int inserttreesumdb(const char *name, sqlite3 *sdb, struct sum *su,int rectype,int uid,int gid)
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
    sprintf(sqlstmt,"INSERT INTO treesummary VALUES (%lld, %lld, %lld, %lld,%lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %lld, %d, %d, %d);",
       su->totsubdirs, su->maxsubdirfiles, su->maxsubdirlinks, su->maxsubdirsize,su->totfiles, su->totlinks, su->minuid, su->maxuid, su->mingid, su->maxgid, su->minsize, su->maxsize, su->totltk, su->totmtk, su->totltm, su->totmtm, su->totmtg, su->totmtt, su->totsize, su->minctime, su->maxctime, su->minmtime, su->maxmtime, su->minatime, su->maxatime, su->minblocks, su->maxblocks,su->totxattr,depth,su->mincrtime, su->maxcrtime, su->minossint1, su->maxossint1, su->totossint1, su->minossint2, su->maxossint2, su->totossint2, su->minossint3, su->maxossint3, su->totossint3, su->minossint4,su->maxossint4, su->totossint4, rectype, uid, gid);

    rc = sqlite3_exec(sdb, sqlstmt, 0, 0, &err_msg);
    if (rc != SQLITE_OK ) {
        fprintf(stderr, "SQL error on insert: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }
    sqlite3_free(err_msg);
    return 0;
}
