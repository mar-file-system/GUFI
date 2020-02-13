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



#include <errno.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "bf.h"
#include "dbutils.h"
#include "utils.h"

extern int errno;

void sub_help() {
   printf("DB_path           path to dir containing %s.*\n",DBNAME);
   printf("SQL               arbitrary SQL on DB\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     char name[MAXPATH];
     char shortname[MAXPATH];
     char endname[MAXPATH];
     char dbname[MAXPATH];
     char rsqlstmt[MAXSQL];
     struct stat statuso;
     int rc;
     sqlite3 *db;
     int recs;
     int dirsummary = 0;        // rawquerydb() ignores this argument

     // sprintf(name,"%s",argv[1]);
     // sprintf(rsqlstmt,"%s",argv[2]);
     // printpath=atoi(argv[3]);
     // printheader=atoi(argv[4]);
     // dirsummary=atoi(argv[5]);

     int idx = parse_cmd_line(argc, argv, "hHNV", 2, "[-s] DB_path SQL", &in);
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;
     else {
        // parse our custom option '-s'
        if ((argc - idx) > 2) {
           if (strcmp(argv[idx], "-s")) {
              print_help(argv[0], "hHNV", "[-s] DB_path SQL");
              return -1;
           }
           dirsummary = 1;
           ++idx;
        }

        // parse positional args following the options
        int retval = 0;
        INSTALL_STR(name,     argv[idx++], MAXPATH, "<DB_path>");
        INSTALL_STR(rsqlstmt, argv[idx++], MAXSQL,  "<SQL>");
        if (retval)
           return retval;
     }
     printf("processing query name %s\n", name);


     // assure we have access to the directory
     rc=lstat(name,&statuso);
     if (rc != 0) {
        printf("ERROR: directory %s: %s\n", name, strerror(errno));
       return 1;
     }
     if (!S_ISDIR(statuso.st_mode)) {
       printf("ERROR: %s not a directory\n",name);
       return 1;
     }

     // assure we have access to the DB
     SNPRINTF(dbname,MAXPATH,"%s/%s",name,DBNAME);
     rc=lstat(dbname,&statuso);
     if (rc != 0) {
        printf("ERROR:  db %s: %s\n", dbname, strerror(errno));
        return 1;
     }


     // run the query
     db = opendb(dbname, RDONLY, 1, 1,
                 NULL, NULL
                 #ifdef DEBUG
                 , NULL, NULL
                 , NULL, NULL
                 , NULL, NULL
                 , NULL, NULL
                 #endif
                 );

     //add query funcs to get uidtouser() gidtogroup() and path()
     addqueryfuncs(db, 0);

     // set the global path so path() is the path passed in
     memset(endname,0,sizeof(endname));
     shortpath(name,shortname,endname);
     //printf("shortpath out shortname %s end name %s\n",shortname, endname);

     // per-thread
     SNPRINTF(gps[0].gepath,MAXPATH,"%s",endname);
     if (dirsummary)
        SNPRINTF(gps[0].gpath,MAXPATH,"%s",shortname);
     else
        SNPRINTF(gps[0].gpath,MAXPATH,"%s",name);

     recs=rawquerydb(name, dirsummary, db, rsqlstmt, in.printing, in.printheader, in.printrows, 0);
     if (recs >= 0)
        printf("query returned %d records\n",recs);
     else
        printf("query returned error\n");

     closedb(db);



     return 0;
}
