#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <stdio.h> 
#include <stdlib.h>
#include <sys/stat.h>
#include <utime.h>
#include <sys/xattr.h>
#include <sqlite3.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>

#include <pwd.h>
#include <grp.h>
//#include <uuid/uuid.h>

#include "bf.h"
#include "structq.h"
#include "utils.h"
#include "dbutils.h"
// #include "putils.h"

// This becomes an argument to thpool_add_work(), so it must return void,
// instead of void*.
static void processdir(void * passv)
{
    struct work *passmywork = passv;
    struct work qwork;
    DIR *dir;
    struct dirent *entry;
    int mytid;
    char *records; 
    sqlite3_stmt *res;   
    sqlite3_stmt *reso;   
    char dbpath[MAXPATH];
    sqlite3 *db;
    sqlite3 *db1;
    int recs;
    char shortname[MAXPATH];
    char endname[MAXPATH];

    // get thread id so we can get access to thread state we need to keep until the thread ends
    mytid=0;
    if (in.outfile > 0) mytid=gettid();
    if (in.outdb > 0) mytid=gettid();

    // open directory
    if (!(dir = opendir(passmywork->name)))
       goto out_free; // return NULL;

    if (!(entry = readdir(dir)))
       goto out_dir; // return NULL;

    sprintf(passmywork->type,"%s","d");
    //if (in.printdir > 0) {
    //  printits(passmywork,mytid);
    //}

    // if we have out db then we have that db open so we just attach the gufi db
    if (in.outdb > 0) {
      db=gts.outdbd[mytid];
      attachdb(passmywork->name,db,"tree");
    } else {
      db=opendb(passmywork->name,db1,1,0);
    }

    // this is needed to add some query functions like path() uidtouser() gidtogroup()
    addqueryfuncs(db);

    recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
             /* if it fails then this will be set to 1 and will go on */

    // if AND operation, and sqltsum is there, run a query to see if there is a match.
    // if this is OR, as well as no-sql-to-run, skip this query
    if (strlen(in.sqltsum) > 1) {

       if (in.andor == 0)       // AND
         recs=rawquerydb(passmywork->name, 0, db, in.sqltsum, 0, 0, 0, mytid);

      // this is an OR or we got a record back. go on to summary/entries
      // queries, if not done with this dir and all dirs below it
    }
    // this means that no tree table exists so assume we have to go on
    if (recs < 0) {
      recs=1;
    }
    // so we have to go on and query summary and entries possibly
    if (recs > 0) {

        // go ahead and send the subdirs to the queue since we need to look
        // further down the tree.  loop over dirents, if link push it on the
        // queue, if file or link print it, fill up qwork structure for
        // each
        do {
           if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
             continue;
           bzero(&qwork,sizeof(qwork));
           sprintf(qwork.name,"%s/%s", passmywork->name, entry->d_name);
           qwork.pinode=passmywork->statuso.st_ino;
           lstat(qwork.name, &qwork.statuso);
           if (S_ISDIR(qwork.statuso.st_mode)) {
              if (!access(qwork.name, R_OK | X_OK)) {
                // this is how the parent gets passed on
                qwork.pinode=passmywork->statuso.st_ino;
                // this pushes the dir onto queue - pushdir does locking around queue update
                pushdir(&qwork);
              }
           }
        } while ((entry = (readdir(dir))));

        // run query on summary, print it if printing is needed, if returns none 
        // and we are doing AND, skip querying the entries db
        // bzero(endname,sizeof(endname));
        shortpath(passmywork->name,shortname,endname);
        sprintf(gps[mytid].gepath,"%s",endname);
        if (strlen(in.sqlsum) > 1) {
          recs=1; /* set this to one record - if the sql succeeds it will set to 0 or 1 */
          // for directories we have to take off after the last slash
          // and set the path so users can put path() in their queries
          sprintf(gps[mytid].gpath,"%s",shortname);
          recs=rawquerydb(passmywork->name, 1, db, in.sqlsum, 1, 0, in.printdir, mytid);
          //printf("summary ran %s on %s returned recs %d\n",in.sqlsum,passmywork->name,recs);
        } else {
          recs=1;
        }
        if (in.andor > 0)
           recs=1;

        // if we have recs (or are running an OR) query the entries table
        if (recs > 0) {
          if (strlen(in.sqlent) > 1) {
            // set the path so users can put path() in their queries
            //printf("****entries len of in.sqlent %lu\n",strlen(in.sqlent));
            sprintf(gps[mytid].gpath,"%s",passmywork->name);
            rawquerydb(passmywork->name, 0, db, in.sqlent, 1, 0, in.printing, mytid);
            //printf("entries ran %s on %s returned recs %d len of in.sqlent %lu\n",
            //       in.sqlent,passmywork->name,recs,strlen(in.sqlent));
          }
        }
    }

    // if we have an out db we just detach gufi db
    if (in.outdb > 0) {
      detachdb(passmywork->name,db,"tree");
    } else {
      closedb(db);
    }

 out_dir:
    // close dir
    closedir(dir);

 out_free:
    // free the queue entry - this has to be here or there will be a leak
    free(passmywork->freeme);

    // one less thread running
    decrthread();

    // return NULL;
}


int processinit(void * myworkin) {
    
     struct work * mywork = myworkin;
     int i;
     char outfn[MAXPATH];
     char outdbn[MAXPATH];
     sqlite3 *dbo;

     //open up the output files if needed
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         sprintf(outfn,"%s.%d",in.outfilen,i);
         gts.outfd[i]=fopen(outfn,"w");
         i++;
       }
     }
     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         sprintf(outdbn,"%s.%d",in.outdbn,i);
         gts.outdbd[i]=opendb(outdbn,dbo,5,0);
         if (strlen(in.sqlinit) > 1) {
           rawquerydb(outdbn, 1, gts.outdbd[i], in.sqlinit, 1, 0, in.printdir, i);
         }
         i++;
       }
     }


     //  ******  create and open output db's here


     // process input directory and put it on the queue
     sprintf(mywork->name,"%s",in.name);
     lstat(in.name,&mywork->statuso);
     if (access(in.name, R_OK | X_OK)) {
         perror("couldn't access input dir");
         return 1;
     }
     if (!S_ISDIR(mywork->statuso.st_mode) ) {
         fprintf(stderr,"not a directory as input\n");
         return 1;
     }

     pushdir(mywork);
     return 0;
}


int processfin() {
int i;

     // close outputfiles
     if (in.outfile > 0) {
       i=0;
       while (i < in.maxthreads) {
         fclose(gts.outfd[i]);
         i++;
       }
     }

     // close output dbs here
     if (in.outdb > 0) {
       i=0;
       while (i < in.maxthreads) {
         closedb(gts.outdbd[i]);
         if (strlen(in.sqlfin) > 1) {
           rawquerydb("fin", 1, gts.outdbd[i], in.sqlfin, 1, 0, in.printdir, i);
         }
         i++;
       }
     }

     return 0;
}



int validate_inputs() {
   return 0;
}

void sub_help() {
   printf("GUFI_tree         find GUFI index-tree here\n");
   printf("\n");
}

int main(int argc, char *argv[])
{
     //char nameo[MAXPATH];
     struct work mywork;
     int i;

     // process input args - all programs share the common 'struct input',
     // but allow different fields to be filled at the command-line.
     // Callers provide the options-string for get_opt(), which will
     // control which options are parsed for each program.
     int idx = parse_cmd_line(argc, argv, "hHT:S:E:Papn:o:d:O:I:F:", 1, "GUFI_tree");
     if (in.helped)
        sub_help();
     if (idx < 0)
        return -1;
     else {
        // parse positional args, following the options
        int retval = 0;
        INSTALL_STR(in.name, argv[idx++], MAXPATH, "GUFI_tree");

        if (retval)
           return retval;
     }

     if (validate_inputs())
        return -1;


     // start threads and loop watching threads needing work and queue size
     // - this always stays in main right here
     mythpool = thpool_init(in.maxthreads);
     if (thpool_null(mythpool)) {
        fprintf(stderr, "thpool_init() failed!\n");
        return -1;
     }

     // process initialization, this is work done once the threads are up
     // but not busy yet - this will be different for each instance of a bf
     // program in this case we are stating the directory passed in and
     // putting that directory on the queue
     processinit(&mywork);

     // processdirs - if done properly, this routine is common and does not
     // have to be done per instance of a bf program loops through and
     // processes all directories that enter the queue by farming the work
     // out to the threadpool
     processdirs(processdir);

     // processfin - this is work done after the threads are done working
     // before they are taken down - this will be different for each
     // instance of a bf program
     processfin();


     // clean up threads and exit
     thpool_wait(mythpool);
     thpool_destroy(mythpool);
     return 0;
}
