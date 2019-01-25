#!/usr/bin/python
## get subprocess module 
## get os module 
## get time module 
import subprocess
import os 
import time

tdir=os.getcwd()
gdir=".."
mtmp="/tmp"
top="/tmp/gitest"
topgt="/tmp/gitest/gt"
toptg="/tmp/gitest/tg"
topfullafter="/tmp/gitest/fullafter"
fa="fullafter"
thegt="gt"
dirs0=[0,1,2]
dirs1=[0,1,2]
dirs2=[0,1,2]
files=[0,1,2]

# initialize a src tree, make a full gufi tree, do a bfq in case we want to compare with the full at initial state, make a full initial tree snap
def ginit():
 print "initializing a tree, full gufi tree, and full tree snap"
 # clean up and make initial directories
 os.system('rm -rf %s' % (top))
 os.system('mkdir %s' % (top))
 os.system('mkdir %s/d0' % (top))
 os.system('mkdir %s/gt' % (top))
 # make the source tree
 for i in dirs0:
   os.system('mkdir %s/d0/d0%d' % (top,i))
   for j in files:
      os.system('touch %s/d0/d0%d/f0%d%d' % (top,i,i,j))
   for k in dirs1:
      os.system('mkdir %s/d0/d0%d/d0%d%d' % (top,i,i,k))
      for l in files:
         os.system('touch %s/d0/d0%d/d0%d%d/f0%d%d%d' % (top,i,i,k,i,k,l))
      for m in dirs2:
         os.system('mkdir %s/d0/d0%d/d0%d%d/d0%d%d%d' % (top,i,i,k,i,k,m))
         for n in files:
            os.system('touch %s/d0/d0%d/d0%d%d/d0%d%d%d/f0%d%d%d%d' % (top,i,i,k,i,k,m,i,k,m,n))

 # wait a second and then put time in the current timestamp file - about to start the full
 time.sleep(1)
 #we have to do most of this from the top directory but sometimes we have to cd to the top of the gufi tree for bfqs
 # to have the same relative diretory structure
 os.chdir(top)
 mytime=str(time.mktime(time.localtime()))
 mysec=mytime.partition(".")
 os.system('echo %s > fulltime' % (mysec[0]))

 # create a gufi tree
 os.system('%s/%s/bfwi -n 2 -x -b -t gt d0' % (tdir,gdir)) 

 # this is just for testing - make a bfq output to compare with if we want to - leave out atime as it may change
 os.chdir(topgt)
 os.system('%s/%s/bfq -n1 -Pp -E "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from entries;" -S "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from vsummarydir;" -a -o %s/fullbfqout d0'  % (tdir,gdir,top)) 

 # make a directory structure snapshot database from the full
 #os.system('%s/%s/bfq -n2 -Pp -S "insert into readdirplus select path()||\'/\'||name,type,inode,pinode,0 from vsummarydir;" -I "CREATE TABLE readdirplus(path TEXT, type TEXT, inode INT64 PRIMARY KEY, pinode INT64, suspect INT64);" -O %s/fullinitsnapdb d0'  % (tdir,gdir,top)) 
 os.system('%s/%s/bfq -n2 -Pp -S "insert into readdirplus select case path()||\'Z\' when \'Z\' then name else path()||\'/\'||name end,type,inode,pinode,0 from vsummarydir;" -I "CREATE TABLE readdirplus(path TEXT, type TEXT, inode INT64 PRIMARY KEY, pinode INT64, suspect INT64);" -O %s/fullinitsnapdb d0'  % (tdir,gdir,top)) 

 os.chdir(top)

 # what have we accomplished
 print "we should have created a source tree d0, a gufi tree gt, a full bfq out to compare to, a full initial snap of the tree structure, and a time stamp"
 #os.system('ls -l %s' % (top))
 print "time after create initial gufi tree sec %s" % (mysec[0])

# this is where incremental of a gufi is done in addition we do a bfq of the incrementally updated gufi tree for comparison
def gincr():
 global suspectopt
 mytime=str(time.mktime(time.localtime()))
 mysec=mytime.partition(".")
 os.system('echo %s > incrtime' % (mysec[0]))
 # get the full time so we can use it to generate new dir snap and suspects 
 fo = open("fulltime", "rb")
 itt = fo.read(12);
 it = itt.rstrip()
 fo.close()
 os.system('mkdir %s' % (toptg))
 #os.system('%s/%s/bfwreaddirplus2db -R -n 2 -A 3 -O incrsnapdb -c %s -x -t %s d0'  % (tdir,gdir,it,toptg))
 os.system('%s/%s/bfwreaddirplus2db -R -n 2 %s -O incrsnapdb -c %s -x -t %s d0'  % (tdir,gdir,suspectopt,it,toptg))
 
 # now that we have initial and incremental snaps we need to do a full outer join to diff these, we need to calculate depths for operation ordering, and we need to create a permanent diff table so we can do several queries against it efficiently
 # attach full and incr dbs one per thread
 fd = open("incrdiff", "a")
 fd.write("attach \'fullinitsnapdb.0\' as full0;\n")
 fd.write("attach \'fullinitsnapdb.1\' as full1;\n")
 fd.write("attach \'incrsnapdb.0\' as incr0;\n")
 fd.write("attach \'incrsnapdb.1\' as incr1;\n")
 # create temp views of all fulls and all incrs via union
 fd.write("create temp view b0 as select * from full0.readdirplus union all select * from full1.readdirplus;\n")
 fd.write("create temp view b1 as select * from incr0.readdirplus union all select * from incr1.readdirplus;\n")
 #create temp view thats a full outer join the sqlite3 way
 fd.write("create temp view jt as select a0.path a0path,a0.type a0type,a0.inode a0inode,a0.pinode a0pinode,a0.suspect a0suspect,a1.path a1path,a1.type a1type,a1.inode a1inode,a1.pinode a1pinode,a1.suspect a1suspect from b0 as a0 left outer join b1 as a1 on a0.inode=a1.inode union all select a0.path a0path ,a0.type a0type,a0.inode a0inode,a0.pinode a0pinode,a0.suspect a0suspect,a1.path a1path,a1.type a1type,a1.inode a1inode,a1.pinode a1pinode,a1.suspect a1suspect from b1 as a1 left outer join b0 as a0 on a1.inode=a0.inode where a0.inode is null;\n")
 # create the output diff table 
 fd.write("create table diff (a0path text,a0type text,a0inode int(64),a0pinode int(64),a0suspect int(64),a1path text,a1type text,a1inode int64,a1pinode int(64),a1suspect int(64),a0depth int(64) ,a1depth int(64));\n")
 # load the diff table from a query of all differences and build depths
 fd.write("insert into diff select *,(length(a0path)-length(replace(a0path ,'/','')))/1 as a0depth,(length(a1path)-length(replace(a1path ,'/','')))/1 as a1depth from jt where a0pinode!=a1pinode or a0path!=a1path or a0inode is null or a1inode is null or a1suspect=1;\n")
 fd.close()
 #build the diff db
 os.system('sqlite3 diffdb \'.read incrdiff\'')
 
 # this allows you to start from the bottom of the current gufi tree and do deletes of directories and moves of directories to a parking lot
 print "delete and move reverse order"
 proc = subprocess.Popen('sqlite3 diffdb \'select a0path,a1path,a1inode,case when a1path is null then 1 else 0 end from diff where a1path is null or a0pinode!=a1pinode or a0path!=a1path order by a0depth desc;\'',stdout=subprocess.PIPE,shell=True)
 for line in proc.stdout:
    a0path=line.split('|')[0]
    a1path=line.split('|')[1]
    a1inode=line.split('|')[2]
    delr=line.split('|')[3]
    mdel=delr.rstrip()
    my_dir="%s/%s" % (topgt,a0path)
    my_lot="%s/d.%s" % (toptg,a1inode)
    #print "a0path %s a1path %s a1inode %s mdel %s src dir %s park dir %s" % (a0path, a1path, a1inode, mdel ,my_dir, my_lot)
    # if delete remove with recursion this dir
    if mdel=='1':
      #del
      print "deleteing subdir %s" % my_dir
      os.system('rm -rf %s' % (my_dir))
    else:
      #move to lot
      print "moving %s to  %s" % (my_dir,my_lot)
      os.system('mv %s %s' % (my_dir,my_lot))
 proc.wait() 
 print "delete and move reverse order done"

 # this allows you to start from the top and add new directories and move directories from the parking lot back into the gufi tree
 print "new and move in order"
 proc = subprocess.Popen('sqlite3 diffdb \'select a0path,a1path,a1inode,case when a0path is null then 1 else 0 end from diff where a0path is null or a0pinode!=a1pinode or a0path!=a1path  order by a1depth asc;\'',stdout=subprocess.PIPE,shell=True)
 for line in proc.stdout:
    a0path=line.split('|')[0]
    a1path=line.split('|')[1]
    a1inode=line.split('|')[2]
    newr=line.split('|')[3]
    mnew=newr.strip() 
    my_dir="%s/%s" % (topgt,a1path)
    my_lot="%s/d.%s" % (toptg,a1inode)
    #print "a0path %s a1path %s a1inode %s newr %s srcdir %s parkdir %s" % (a0path, a1path, a1inode, mnew, my_dir, my_lot)
    # if new add the directory
    if mnew=='1':
      #add new dir assume we will fix the permissions later when we populate it
      print "mkdir %s" % (my_dir)
      os.system('mkdir %s' % (my_dir))
    else:
      #move from lot
      print "moving back %s to  %s" % (my_lot,my_dir)
      os.system('mv %s %s' % (my_lot,my_dir))
 proc.wait() 
 print "new and move in order done"

 # this tells you what gufi dbs are in the temp area from bfwreaddirplus2db walk that need to be put into the gufi tree/replace/new and potentially 
 # update the name of the dir, the mode/owner/group of the dir
 print "update"
 proc = subprocess.Popen('sqlite3 diffdb \'select a0path, a1path, case when a0path=a1path then 0 else 1 end, a1inode, a1pinode, case when a0pinode=a1pinode then 0 else 1 end from diff where a1path is not null order by a1depth asc;\'',stdout=subprocess.PIPE,shell=True)
 for line in proc.stdout:
    #the real code does filtering here
    a0path=line.split('|')[0]
    a1path=line.split('|')[1]
    rename=line.split('|')[2]
    a1inode=line.split('|')[3]
    a1pinode=line.split('|')[4]
    mover=line.split('|')[5]
    move=mover.rstrip()
    #print "a0path %s a1path %s rename %s a1inode %s a1pinode %s move %s" % (a0path, a1path, rename, a1inode, a1pinode, move)
    my_file="%s/%s" % (toptg,a1inode)
    exists=os.path.isfile(my_file) 
    if exists:
      #print "file %s found" % (my_file)
      sqlstmt="sqlite3 %s \'select mode, uid, gid from vsummarydir;\'" % (my_file)
      pproc = subprocess.Popen(sqlstmt,stdout=subprocess.PIPE,shell=True)
      for pline in pproc.stdout:
         a1mode=pline.split('|')[0]
         a1uid=pline.split('|')[1]
         a1gidr=pline.split('|')[2]
         a1gid=a1gidr.rstrip()
         #print "a1mode %s a1uid %s a1gid %s" % (a1mode, a1uid, a1gid)
         # we copy/move the gufi into this dir
         topath='%s/%s' % (thegt,a1path)
         #print "moving %s to  %s" % (frompath,topath)
         #os.system('mv %s %s' % (frompath,topath))
         # we set the mode, uid and gid
         # go through the mode bits conversion
         # we copy/move the gufi into this dir
         i1mode=int(a1mode)
         o1mode=oct(i1mode & 0o777)[-3:]
         c1mode='0o%s' % o1mode
         mfrom='%s/%s' % (toptg,a1inode)
         mto='%s/db.db' % (topath)
         print "chmod chown of %s mode %s uid %s gid %s move %s to %s" % (topath,c1mode,a1uid,a1gid,mfrom,mto)
         i1uid=int(a1uid)
         i1gid=int(a1gid)
         os.rename(mfrom,mto)
         os.chmod(topath,i1mode)
         os.chown(topath, i1uid, i1gid)
         os.chmod(mto,i1mode)
         os.system('chmod ugo-x %s' % (mto))
         os.chown(mto, i1uid, i1gid)
      pproc.wait()
    else:
      print "file %s not found - probably just a move a0path %s a1path %s" % (my_file,a0path,a1path)
 proc.wait() 
 print "update done"
 os.chdir(topgt)
 # leave out atime
 os.system('%s/%s/bfq -n1 -Pp -E "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from entries;" -S "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from vsummarydir;" -a -o %s/incrbfqoutafter d0'  % (tdir,gdir,top)) 
 os.chdir(top)

# make a full gufi tree of the src tree after mods then do a bfq of that for comparison with an incrementally updated gufi tree 
def gfullafter():

 os.system('mkdir %s' % (topfullafter))
 os.system('%s/%s/bfwi -n 2 -x -b -t %s d0' % (tdir,gdir,fa)) 
 os.chdir(topfullafter)
 #leave out atime
 os.system('%s/%s/bfq -n1 -Pp -E "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from entries;" -S "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from vsummarydir;" -a -o %s/fullbfqoutafter d0'  % (tdir,gdir,top)) 
 os.chdir(top)


##################################  main  ############################################
suspectopt=" "
################################### test1 ############################################
testn="change contents of one file"
# this cleans up and makes a fresh gufi
# you have to not be in the top temp directory as this deletes that and rebuilds it
os.chdir(mtmp)
ginit()
# set the dir back to top temp directory
os.chdir(top)

# wait a bit and make a change
time.sleep(1)

###########incremental test
print "---------- test %s start" % testn
os.system('echo cheese >> d0/d00/d000/f0000')
#############################

# do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
gfullafter()
# wait until incremental time
time.sleep(1)
# do incremental update of gufi tree
suspectopt="-A 3"
gincr()
# gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
exit_code = os.WEXITSTATUS(cmd)
print "---------- test %s done result %d" % (testn,exit_code)
#end incremental test
################################### test1 end ########################################
################################### test2 ############################################
testn="delete directory segment"
# this cleans up and makes a fresh gufi
# you have to not be in the top temp directory as this deletes that and rebuilds it
os.chdir(mtmp)
ginit()
# set the dir back to top temp directory
os.chdir(top)

# wait a bit and make a change
time.sleep(1)

############incremental test
print "---------- test %s start" % testn
os.system('rm -rf d0/d00/d000')
#############################

# do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
gfullafter()
# wait until incremental time
time.sleep(1)
# do incremental update of gufi tree
suspectopt="-A 3"
gincr()
# gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
cmd=exit_code = os.WEXITSTATUS(cmd)
print "---------- test %s done result %d" % (testn,exit_code)
#end incremental test
################################### test2 end ########################################
################################### test3 ############################################
testn="move a directory segment"
# this cleans up and makes a fresh gufi
# you have to not be in the top temp directory as this deletes that and rebuilds it
os.chdir(mtmp)
ginit()
# set the dir back to top temp directory
os.chdir(top)

# wait a bit and make a change
time.sleep(1)

############incremental test
print "---------- test %s start" % testn
os.system('mv d0/d00/d000 d0')
#############################

# do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
gfullafter()
# wait until incremental time
time.sleep(1)
# do incremental update of gufi tree
suspectopt="-A 3"
gincr()
# gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
exit_code = os.WEXITSTATUS(cmd)
print "---------- test %s done result %d" % (testn,exit_code)
#end incremental test
################################### test3 end ########################################
################################### test4 ############################################
testn="add a directory segment"
# this cleans up and makes a fresh gufi
# you have to not be in the top temp directory as this deletes that and rebuilds it
os.chdir(mtmp)
ginit()
# set the dir back to top temp directory
os.chdir(top)

# wait a bit and make a change
time.sleep(1)

############incremental test
print "---------- test %s start" % testn
os.system('mkdir d0/d00/nd000')
os.system('touch d0/d00/nd000/fnd000')
os.system('mkdir d0/d00/nd000/nd0000')
os.system('touch d0/d00/nd000/nd0000/fnd0000')

#############################

# do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
gfullafter()
# wait until incremental time
time.sleep(1)
# do incremental update of gufi tree
suspectopt="-A 3"
gincr()
# gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
exit_code = os.WEXITSTATUS(cmd)
print "---------- test %s done result %d" % (testn,exit_code)
#end incremental test
################################### test4 end ########################################
################################### test5 ############################################
testn="add, delete, move multiple segments and touch an existing file"
# this cleans up and makes a fresh gufi
# you have to not be in the top temp directory as this deletes that and rebuilds it
os.chdir(mtmp)
ginit()
# set the dir back to top temp directory
os.chdir(top)

# wait a bit and make a change
time.sleep(1)

############incremental test
print "---------- test %s start" % testn
os.system('mkdir d0/d00/nd000')
os.system('touch d0/d00/nd000/fnd000')
os.system('mkdir d0/d00/nd000/nd0000')
os.system('touch d0/d00/nd000/nd0000/fnd0000')
os.system('mv d0/d00/d000 d0')
os.system('mv d0/d000/d0000 d0')
os.system('mv d0/d000 d0/d0000')
os.system('rm -rf d0/d01/d000')
os.system('echo cheese >> d0/d02/d020/f0200')

#############################

# do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
gfullafter()
# wait until incremental time
time.sleep(1)
# do incremental update of gufi tree
suspectopt="-A 3"
gincr()
# gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
exit_code = os.WEXITSTATUS(cmd)
print "---------- test %s done result %d" % (testn,exit_code)
#end incremental test
################################### test5 end ########################################
################################### test6 ############################################
testn="change an existing file using suspect file for file suspects"
# this cleans up and makes a fresh gufi
# you have to not be in the top temp directory as this deletes that and rebuilds it
os.chdir(mtmp)
ginit()
# set the dir back to top temp directory
os.chdir(top)

# wait a bit and make a change
time.sleep(1)

############incremental test
print "---------- test %s start" % testn
os.system('echo cheese >> d0/d02/d020/f0200')

#############################

# do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
gfullafter()
# wait until incremental time
time.sleep(1)
# do incremental update of gufi tree
# since we are using suspect mode 2 where we provide a list of files that are suspect we have to put the inode and type in a file
ttpath="d0/d02/d020/f0200"
mstat = os.stat(ttpath)
mksus="echo %s f >> %s/suspects" % (mstat.st_ino,top)
os.system(mksus)
# now that we have a suspect file we tell bfwreaddirplus2db to use that for file suspects (stat dirs but use suspect file for files/links
#suspectopt="-A 3"
suspectopt="-A 2 -W %s/suspects " % (top)
#print "%s inode is %s suspectopts %s" % (ttpath,mstat.st_ino,suspectopt)
gincr()
# gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
exit_code = os.WEXITSTATUS(cmd)
print "---------- test %s done result %d" % (testn,exit_code)
#end incremental test
################################### test6 end ########################################
