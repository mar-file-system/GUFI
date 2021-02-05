#!/usr/bin/python
## get subprocess module
## get os module
## get time module
import subprocess
import os
import time
import sys
import string

tdir=os.getcwd()
sdir="%s/../scripts" % (tdir)
gdir=".."
mtmp="/tmp"
funcout="/tmp/gufitestfuncout"
top="/tmp/gufitest"
topgt="%s/gt" % (top)
toptg="%s/tg" % (top)
fa="fullafter"
thegt="gt"
dirs0=[0,1,2]
dirs1=[0,1,2]
dirs2=[0,1,2]
files=[0,1,2]
dcnt=1
lcnt=0
fcnt=0
xcnt=0
fsz=0

# From Stack Overflow
# https://stackoverflow.com/a/377028
# by Jay (https://stackoverflow.com/users/20840/jay)
def which(program):
    import os
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

def wipe():
 print "wiping test tree"
 os.system('rm -rf %s' % (top))
 os.system('mkdir %s' % (top))
 os.system('mkdir %s' % (topgt))
 os.system('mkdir %s/d0' % (topgt))

def gtfromwalk(mt,st,tt):
 # create a gufi tree
 mwd=os.getcwd()
 os.chdir('%s' % (tt))
 print "creating a gufi tree from walk at %s from %s" % (mt,st)
 os.system('mkdir %s/%s' % (top,mt))
 if which("xattr"):
  os.system('xattr -c %s/%s' % (top,mt))
 os.system('%s/%s/bfwi -n 2 -x -b -t %s %s' % (tdir,gdir,mt,st))
 os.chdir('%s' % (mwd))

def writetimef(tf):
 print "write time stamp in %s/%s" % (top,tf)
 mytime=str(time.mktime(time.localtime()))
 mysec=mytime.partition(".")
 os.system('echo %s > %s/%s' % (mysec[0],top,tf))

def mkbfqout(tt,outf,ti):
 # this is just for testing - make a bfq output to compare with if we want to - leave out atime as it may change
 print "making bfqoutfile %s/%s of %s/%s" % (top,outf,tt,ti)
 mwd=os.getcwd()
 os.chdir('%s' % (tt))
 os.system('%s/%s/bfq -n1 -Pp -E "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from entries;" -S "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from vsummarydir;" -a -o %s/%s %s'  % (tdir,gdir,top,outf,ti))
 os.system('sort -o %s/%s.0 %s/%s.0' % (top,outf,top,outf))
 os.chdir('%s' % (mwd))

def mksrctree():
 # make the source tree
 global dcnt
 global fcnt
 global fsz
 global lcnt
 global xcnt
 print "make the source gtree at %s/d0" % (top)
 os.system('mkdir %s/d0' % (top))
 dcnt=1
 fcnt=0
 fsz=0
 xcnt=0
 lcnt=0
 for i in dirs0:
   os.system('mkdir %s/d0/d0%d' % (top,i))
   dcnt+=1
   for j in files:
      os.system('touch %s/d0/d0%d/f0%d%d' % (top,i,i,j))
      fcnt+=1
   for k in dirs1:
      os.system('mkdir %s/d0/d0%d/d0%d%d' % (top,i,i,k))
      dcnt+=1
      for l in files:
         os.system('touch %s/d0/d0%d/d0%d%d/f0%d%d%d' % (top,i,i,k,i,k,l))
         fcnt+=1
      for m in dirs2:
         os.system('mkdir %s/d0/d0%d/d0%d%d/d0%d%d%d' % (top,i,i,k,i,k,m))
         dcnt+=1
         for n in files:
            os.system('touch %s/d0/d0%d/d0%d%d/d0%d%d%d/f0%d%d%d%d' % (top,i,i,k,i,k,m,i,k,m,n))
            fcnt+=1
 # add some odd stuff like bytes into files, links to files, links to dirs, names with odd characters and spaces
 os.system('echo cheese >> %s/d0/d01/d010/f0100' % (top))
 fsz=fsz+7
 os.system('echo cheese >> %s/d0/d02/d020/f0200' % (top))
 fsz=fsz+7
 # try to run xattr if it is available
 if which("xattr"):
    os.system('xattr -w cheese cheddar %s/d0/d00/d000/f0000' % (top))
 elif which("attr"):
    os.system('attr -s cheese -V cheddar %s/d0/d00/d000/f0000' % (top))
 xcnt+=1
 os.system('ln -s %s/d0/d00/f000 %s/d0/d01/flinkd0d00f000' % (top,top))
 lcnt+=1
 os.system('ln -s %s/d0/d00/d000 %s/d0/d01/dlinkd0d00d000' % (top,top))
 lcnt+=1
 oddname="apost\\'trophe"
 os.system('touch %s/d0/d01/d010/%s' % (top,oddname))
 fcnt+=1
 spacename="space\ directory"
 os.system('mkdir %s/d0/d01/d010/%s' % (top,spacename))
 dcnt+=1
 #add some stuff for legacy reasons
 os.system('ln -s %s/d0 %s/testdir' % (top,top))
 os.system('ln -s %s/gt %s/testdirdup' % (top,top))
 os.system('ln -s %s/gt/d0 %s/gt/testdir' % (top,top))
 os.system('touch %s/testdir/a' % (top))
 fcnt+=1
 os.system('touch %s/testdir/b' % (top))
 fcnt+=1
 os.system('mkdir %s/testdir/c' % (top))
 dcnt+=1
 print "made the source gtree at %s/d0 totdirs=%d totfiles=%d totbytes=%d totlinks=%d totxattr=%d" % (top,dcnt,fcnt,fsz,lcnt,xcnt)

def fsnap(dd,sf):
 # make a directory structure snapshot database from the first full gufi tree
 mwd=os.getcwd()
 os.chdir('%s' % (dd))
 os.system('%s/%s/bfq -n2 -Pp -S "insert into readdirplus select case path()||\'Z\' when \'Z\' then name else path()||\'/\'||name end,type,inode,pinode,0 from vsummarydir;" -I "CREATE TABLE readdirplus(path TEXT, type TEXT, inode INT64 PRIMARY KEY, pinode INT64, suspect INT64);" -O %s/%s d0'  % (tdir,gdir,top,sf))
 os.chdir('%s' % (mwd))

# initialize a src tree, make a full gufi tree, do a bfq compare with the full at initial state, make a full initial tree snap
def ginit():
 print "initializing a tree, full gufi tree, and full tree snap"
 # just start from scratch
 wipe()
 # make a src tree
 mksrctree()
 # wait a second and then put time in the current timestamp file - about to start the full
 time.sleep(1)
 # to have the same relative diretory structure
 os.chdir(top)
 #write timestamp for our first full tree create to come next
 writetimef('fulltime')
 # create a gufi tree
 gtfromwalk('gt','d0',top)
 # make a bfqoutput to compare
 mkbfqout(topgt,'fullbfqout','d0')
 # do a full snap of the dir tree dirs only using bfq (for initial after creating initial full gufi tree
 fsnap(topgt,'fullinitsnapdb')
 # what have we accomplished
 print "created a source tree d0, a gufi tree gt, a full bfq out to compare to, a full initial snap of the tree structure, and a time stamp"
 #os.system('ls -l %s' % (top))

def snapincr(suspectopt,suspf):
 # get the full time so we can use it to generate new dir snap and suspects
 fo = open("fulltime", "rb")
 itt = fo.read(12);
 it = itt.rstrip()
 fo.close()
 os.system('mkdir %s' % (toptg))
 if (suspectopt==3):
   os.system('%s/%s/bfwreaddirplus2db -R -n 2 -A %d -O incrsnapdb -c %s -x -t %s d0'  % (tdir,gdir,suspectopt,it,toptg))
 else:
   os.system('%s/%s/bfwreaddirplus2db -R -n 2 -A %d -W %s/%s -O incrsnapdb -c %s -x -t %s d0'  % (tdir,gdir,suspectopt,top,suspf,it,toptg))

def builddiffdb():
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

def reversedelmoveout():
 #from bottom to top of pre incr snap delete and move dirs to parking lot
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

def forwardnewmovein():
 #from top bottom of post incr snap add new dirs and move dirs back from parking lot
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

def updategufi():
 # process any changes to any dir by moving gufi db's back in from the bfwreaddirplus2db parking lot and chmod/chown dirs
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
 print "incremental gufi tree update done"

# this is where incremental of a gufi is done in addition we do a bfq of the incrementally updated gufi tree for comparison
def gincr(suspectopt,suspf):
 #write timestamp for our incremental tree update
 writetimef('incrtime')

 ############ start of gufi incremental processing ##########
 # get the former time stamp, run dfwreaddirplus2db and create directory snap shots with suspects after mods to the source tree
 snapincr(suspectopt,suspf)

 # now that we have initial and incremental snaps we need to do a full outer join to diff these,
 # we need to calculate depths for operation ordering, and we need to create a permanent diff table
 # so we can do several queries against it efficiently
 builddiffdb()

 # now start from the bottom of the current gufi tree and do deletes of directories and moves of directories to a parking lot
 reversedelmoveout()

 # this allows you to start from the top and add new directories and move directories from the parking lot back into the gufi tree
 forwardnewmovein()

 # this tells you what gufi dbs are in the temp area from bfwreaddirplus2db walk
 #that need to be put into the gufi tree/replace/new and potentially
 # update the name of the dir, the mode/owner/group of the dir
 updategufi()

 ############ end of gufi incremental processing ##########

 # make a bfqout to compare of incrementally updated gufi
 mkbfqout(topgt,'incrbfqoutafter','d0')

# make a full gufi tree of the src tree after mods then do a bfq of that for comparison with an incrementally updated gufi tree
def gfullafter():

 # make a full gufi tree from source tree using bfwi walk after changes to source tree for compare
 gtfromwalk(fa,'d0',top)
 # make a bfqout to compare
 mkbfqout(fa,'fullbfqoutafter','d0')

def cleanstart(testname,sl):
 # this cleans up and makes a fresh gufi
 # you have to not be in the top temp directory as this deletes that and rebuilds it
 print "---------- test %s start" % testname
 os.chdir(mtmp)
 ginit()
 # set the dir back to top temp directory
 os.chdir(top)
 # wait a bit and make a change
 time.sleep(sl)

# make a full gufi tree from a dfw output flat file to compare with original bfwi created gufi tree
def gfullfromdfw(td,id,):
 mwd=os.getcwd()
 os.chdir(top)
 os.system('mkdir %s' % (td))
 os.system('%s/%s/dfw -s -x -d \'|\' -l %s > %s/load.out' % (tdir,gdir,id,top))
 os.system('sort -t \'|\' -k17,17 -k18,18 -o %s/load.srt %s/load.out' % (top,top))
 os.system('%s/%s/bfwi -n 2 -d \'|\' -x -b -t %s -u %s/load.srt' % (tdir,gdir,td,top))
 print "created gufi with dfw output"
 mkbfqout(td,td,'d0')
 os.chdir(mwd)

# make a full gufi tree from find exec dfw flat file to compare with original bfwi create gufi tree
def gfullfromfinddfw(td,id):
 mwd=os.getcwd()
 os.chdir(top)
 os.system('mkdir %s' % (td))
 #os.system('%s/%s/dfw -s -x -d \'|\' -l %s > %s/load.out' % (tdir,gdir,id,top))
 os.system('find %s -exec %s/%s/dfw -s -x -d \'|\' -l -D {} \; >> %s/load.out' % (id,tdir,gdir,top))
 os.system(' sort -t \'|\' -k17,17 -k18,18 -o %s/load.srt %s/load.out' % (top,top))
 os.system('%s/%s/bfwi -n 2 -d \'|\' -x -b -t %s -u %s/load.srt' % (tdir,gdir,td,top))
 print "created gufi with find dfw output"
 mkbfqout(td,td,'d0')
 os.chdir(mdw)

# make a full gufi tree from a bfwi output flat file to compare with original bfwi created gufi tree
def gfullfrombfwiflat(td,id):
 mwd=os.getcwd()
 os.chdir(top)
 os.system('mkdir %s' % (td))
 os.system('%s/%s/bfwi -n 2 -d \'|\' -x -Pp -o %s/load %s' % (tdir,gdir,top,id))
 os.system('cat %s/load.0 %s/load.1 > %s/load.out ' % (top,top,top ))
 os.system('%s/%s/bfwi -n 2 -d \'|\' -x -b -t %s -u %s/load.out' % (tdir,gdir,td,top))
 print "created gufi with bfwi flat output"
 mkbfqout(td,td,'d0')
 os.chdir(mwd)

################################### test cincr3modf ############################################
def cincr3modf():
 testn="correctness incremental mode 3 mod file"
 cleanstart(testn,1)

 ###########incremental test
 os.system('echo cheese >> d0/d00/d000/f0000')
 #############################

 # do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
 gfullafter()
 # wait until incremental time
 time.sleep(1)
 # do incremental update of gufi tree
 gincr(3,'')
 # gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
 print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end incremental test
################################### test cincr3modf end ########################################
################################### test cincr3deld ############################################
def cincr3deld():
 testn="incremental mode 3 del directory segment"
 cleanstart(testn,1)

 ############incremental test
 os.system('rm -rf d0/d00/d000')
 #############################

 # do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
 gfullafter()
 # wait until incremental time
 time.sleep(1)
 # do incremental update of gufi tree
 gincr(3,'')
 # gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
 print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
 cmd=exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end incremental test
################################### test cincr3deld end ########################################
################################### test cincr3movd ############################################
def cincr3movd():
 testn="correctness incremental mode 3 mov directory segment"
 cleanstart(testn,1)

 ############incremental test
 os.system('mv d0/d00/d000 d0')
 #############################

 # do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
 gfullafter()
 # wait until incremental time
 time.sleep(1)
 # do incremental update of gufi tree
 gincr(3,'')
 # gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
 print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end incremental test
################################### test cincr3movd end ########################################
################################### test cincr3addd ############################################
def cincr3addd():
 testn="correctness incremental mode 3 add directory segment"
 cleanstart(testn,1)

 ############incremental test
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
 gincr(3,'')
 # gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
 print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end incremental test
################################### test cincr3addd end ########################################
################################### test cincr3adddelmovdf ############################################
def cincr3adddelmovdf():
 testn="correctness incremental mode 3 add, delete, move multiple dir segments and touch an existing file"
 cleanstart(testn,1)

 ############incremental test
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
 #  do incremental update of gufi tree
 gincr(3,'')
 # gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
 print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end incremental test
################################### test cincr3adddelmovdf end ########################################
################################### test cincr2modf ############################################
def cincr2modf():
 testn="correctness incremental mode 2 mod file (using suspect file for file suspects)"
 cleanstart(testn,1)

 ############incremental test
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
 #print "%s inode is %s suspectopts %s" % (ttpath,mstat.st_ino,suspectopt)
 gincr(2,'suspects')
 # gincr makes a bfq of gufi tree after incr update so we can compare the full gufi tree after change and incr gufi tree after change
 print "++++++++++ comparing bfq from fullafter and bfq from incrafter"
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end incremental test
################################### test cincr2modf end ########################################
################################### test cflatloaddfw ############################################
def cflatloaddfw():
 testn="correctness flat load using bfwi -u from dfw"
 cleanstart(testn,1)
 # do a full gufi tree from dfw flat file
 gfullfromdfw('fullfromdfw','d0')
 print "++++++++++ comparing fulbfq to  fullfromdfw,  original bfwi produced gufi and dfw flat file loaded with bfwi produced gufi"
 #you have to sort these as bfq will walk in entry order and bfwi will produce a gufi in some order and it might be a different order using different modes
 os.system('sort -o %s/fullbfqout.srt %s/fullbfqout.0' % (top,top))
 os.system('sort -o %s/fullfromdfw.srt %s/fullfromdfw.0' % (top,top))
 cmd=os.system('cmp %s/fullbfqout.srt %s/fullfromdfw.srt' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
################################### testcflatloaddfw end ############################################
################################## test cflatloadfinddfw ############################################
def cflatloadfinddfw():
 testn="correctness flat load bfwi -u from find dfw"
 cleanstart(testn,1)
 # do a full gufi tree from find dfw flat file
 gfullfromdfw('fullfromfinddfw','d0')
 print "++++++++++ comparing fulbfq to  fullfromfinddfw, original bfwi produced gufi and find dfw flat file loaded with bfwi produced gufi"
 #you have to sort these as bfq will walk in entry order and bfwi will produce a gufi in some order and it might be a different order using different modes
 os.system('sort -o %s/fullbfqout.srt %s/fullbfqout.0' % (top,top))
 os.system('sort -o %s/fullfromfinddfw.srt %s/fullfromfinddfw.0' % (top,top))
 cmd=os.system('cmp %s/fullbfqout.srt %s/fullfromfinddfw.srt' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
################################## test cflatloadfinddfw end ######################################
################################### test cflatloadbfwi ############################################
def cflatloadbfwi():
 testn="correctness flat load bfwi -u from bfwi"
 cleanstart(testn,1)
 # do a full gufi tree from bfwi flat file
 gfullfrombfwiflat('fullfrombfwiflat','d0')
 print "++++++++++ comparing fulbfq to  fullfrombfwiflat, original bfwi produced gufi and bfwi flat file loaded with bfwi produced gufi"
 #you have to sort these as bfq will walk in entry order and bfwi will produce a gufi in some order and it might be a different order using different modes
 os.system('sort -o %s/fullbfqout.srt %s/fullbfqout.0' % (top,top))
 os.system('sort -o %s/fullfrombfwiflat.srt %s/fullfrombfwiflat.0' % (top,top))
 cmd=os.system('cmp %s/fullbfqout.srt %s/fullfrombfwiflat.srt' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
################################### test cflatloadbfwi end ############################################
################################### test cbfqbftiquerydbs ############################################
def cbfqbftiquerydbs():
 global dcnt
 global fcnt
 global fsz
 global lcnt
 testn="correctness bfq bfti"
 #############################
 # make a fresh src tree and make a few changes
 cleanstart(testn,1)
 #make some tree indexes
 os.system('%s/%s/bfti -n 2 -s gt/d0/d00 > %s/bfti.0' % (tdir,gdir,top))
 os.system('%s/%s/bfti -n 2 -s gt/d0/d01 > %s/bfti.1' % (tdir,gdir,top))
 os.system('%s/%s/bfti -n 2 -s gt/d0/d02 > %s/bfti.2' % (tdir,gdir,top))
 exitcd=0
 cmd=os.system('ls -l %s/bfti.0' % (top))
 exit_code = os.WEXITSTATUS(cmd)
 exitcd=exitcd+exit_code
 cmd=os.system('ls -l %s/bfti.1' % (top))
 exit_code = os.WEXITSTATUS(cmd)
 exitcd=exitcd+exit_code
 cmd=os.system('ls -l %s/bfti.2' % (top))
 exit_code = os.WEXITSTATUS(cmd)
 exitcd=exitcd+exit_code
 if (exitcd!=0):
   exit(exitcd)
 ############################
 # do some bfq tests now
 print "testing bfq tot dirs %d tot files %d tot bytes %d tot files with xattrs %d tot links %d" % (dcnt,fcnt,fsz,xcnt,lcnt)
 of="%s/cbfqbftiquerydbsout" % (top)
 of1="%s/cbfqbftiquerydbsout1" % (top)
 oft="%s/cbfqbftiquerydbsout.0" % (top)
 oft1="%s/cbfqbftiquerydbsout1.0" % (top)
 ###### num file test #####
 print "+++++++++ test to see if gufi agrees totfiles=%d" % (fcnt)
 os.system('%s/%s/bfq -n1 -p -E "select name from entries where type=\'f\';" -o %s %s'  % (tdir,gdir,of,'gt/d0'))
 numl = sum(1 for line in open(oft))
 if (numl != fcnt):
  print "file count missmatch fcnt %d numl %d" % (fcnt,numl)
  exit()
 else:
  print "file count match fcnt %d numl %d" % (fcnt,numl)
 ###### end num file test #####
 ###### num dir test #####
 print "+++++++++ test to see if gufi agrees totdirs=%d" % (dcnt)
 os.system('%s/%s/bfq -n1 -P -S "select name from vsummarydir where type=\'d\';" -o %s %s'  % (tdir,gdir,of,'gt/d0'))
 numl = sum(1 for line in open(oft))
 if (numl != dcnt):
  print "dir count missmatch dcnt %d numl %d" % (dcnt,numl)
  exit()
 else:
  print "dir count match dcnt %d numl %d" % (dcnt,numl)
 ###### end num dir test #####
 ###### num link test #####
 print "+++++++++ test to see if gufi agrees totlinks=%d" % (lcnt)
 os.system('%s/%s/bfq -n1 -Pp -E "select name from entries where type=\'l\';" -o %s %s'  % (tdir,gdir,of,'gt/d0'))
 numl = sum(1 for line in open(oft))
 if (numl != lcnt):
  print "link count missmatch lcnt %d numl %d" % (lcnt,numl)
  exit()
 else:
  print "link count match lcnt %d numl %d" % (lcnt,numl)
 ###### end num dir test #####
 ###### num link test #####
 print "+++++++++ test to see if gufi agrees totxattrs=%d" % (xcnt)
 if which("xattr"):
  os.system('%s/%s/bfq -n1 -Pp -a -S "select name from vsummarydir where length(xattrs)>0;" -E "select name from entries where length(xattrs)>0;" -o %s %s'  % (tdir,gdir,of,'gt/d0'))
  numl = sum(1 for line in open(oft))
  if (numl != xcnt):
   print "xattr count missmatch xcnt %d numl %d" % (xcnt,numl)
   exit()
  else:
   print "xattr count match xcnt %d numl %d" % (xcnt,numl)
 ###### end num dir test #####
 ###### sum of file sizes test uses querydbs #####/os
 print "+++++++++ test to see if bfq creating output db per thread and use querydbs to aggregate total size=%d" % (fsz)
 os.system('%s/%s/bfq -n 2 -p -E "insert into sument select size from entries where type=\'f\'and size>0;" -I "create table sument (sz int64);" -O outdb %s'  % (tdir,gdir,'gt/d0'))
 #os.system('%s/%s/bfq -n 1 -p -E "insert into sument select size from entries where type=\'f\'and size>0;" -I "create table sument (sz int64);" -O outdb %s'  % (tdir,gdir,'gt/d0'))
 os.system('%s/%s/querydbs -V  outdb 2 "select sum(sz) from vsument" sument > querydbsout' % (tdir,gdir))
 #os.system('%s/%s/querydbs -V  outdb 1 "select sum(sz) from vsument" sument > querydbsout' % (tdir,gdir))
 os.system('cat querydbsout');
 proc=subprocess.Popen('head -1 querydbsout',stdout=subprocess.PIPE,shell=True)
 for line in proc.stdout:
  csz=line.split('|')[0]
  sz=string.atoi(csz)
 if (sz!=fsz):
  print "error: file size total from querydbs %d real file size total %d" % (sz,fsz)
  exit()
 else:
  print "good news: file size total from querydbs %d real file size total %d" % (sz,fsz)
 ###### end sum of file sizes test uses querydbs #####
 ######  tree directory use test #####
 ####?????????????????????? need to fix bfq so that it doesnt print an error if a dir doesnt have a tree summary
 #### select name from sqlite_master where type='table' and name='treesummary'; should do the trick
 print "+++++++++ test to see if gufi agrees totxattrs=%d" % (xcnt)
 os.system('%s/%s/bfq -n 1 -Pp -T "select totsize from vtsummarydir where totsize>0" -S "select name from vsummarydir;" -E "select name,type,size from entries where type=\'f\'and size>0;" -o %s %s'  % (tdir,gdir,of,'gt/d0'))
 numl = sum(1 for line in open(oft))
 os.system('%s/%s/bfq -n 1 -Pp  -S "select name from vsummarydir;" -E "select name,type,size from entries where type=\'f\' and size>0;" -o %s %s'  % (tdir,gdir,of1,'gt/d0'))
 numl1 = sum(1 for line in open(oft1))
 if (numl>=numl1):
   print "error: using tree summary examined %d directories and not using tree summary examined %d directories" % (numl,numl1)
   exit()
 else:
   print "good news: using tree summary examined %d directories and not using tree summary examined %d directories" % (numl,numl1)
 ######  end tree directory use test #####
################################## test cbfqbftiquerydbs end ############################################
################################### test cincr3modf ############################################
def cbfwiinsrc():
 testn="correctness bfwi in src tree"
 cleanstart(testn,1)

 # do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
 gfullafter()
 # wait until incremental time
 time.sleep(1)
 # make a full gufi tree and put it right in the source tree
 os.system('%s/%s/bfwi -n 2 -x -b -t %s %s' % (tdir,gdir,'.','d0'))
 mkbfqout('.','fullinsrc','d0')
 print "++++++++++ comparing bfq from in src tree gufi and bfq from from not in src gufi tree "
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/fullinsrc.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end cbfwiinsrc
################################### test  cbfwiinsrc end ########################################
################################### test cbfwrp2dbfullinsrc ############################################
def cbfwrp2dbfullinsrc():
 testn="correctness bfwreaddirplus2db full gufi tree build in src tree"
 cleanstart(testn,1)
 # do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
 gfullafter()
 # wait until incremental time
 time.sleep(1)
 # make a full gufi tree and put it right in the source tree using bfwreaddirplus2db -b full everything will not have db and be suspect so dont need -Y
 #os.system('%s/%s/bfwreaddirplus2db -n 2 -Y -b -x d0'  % (tdir,gdir))
 os.system('%s/%s/bfwreaddirplus2db -n 2 -b -x d0'  % (tdir,gdir))
 mkbfqout('.','fullinsrc','d0')
 print "++++++++++ comparing bfq from in src tree gufi and bfq from from not in src gufi tree "
 cmd=os.system('cmp %s/fullbfqoutafter.0 %s/fullinsrc.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end  cbfwrp2dbfullinsrc
################################### test  cbfwrp2dbfullinsrc end ########################################
################################### test cbfwrdp2dbincrinsrc ############################################
def cbfwrdp2dbincrinsrc():
 testn="correctness bfwreaddirplus2db incr gufi tree update in src tree"
 cleanstart(testn,1)
 #make a gufi full in src tree
 os.system('%s/%s/bfwreaddirplus2db -n 2 -Y -b -x d0'  % (tdir,gdir))
 # wait until incremental time
 time.sleep(1)
 ############incremental test
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
 # the problem with just doing a bfwi to create a gufi from a src tree with a gufi in it is it will have the database files in it
 # so we need to exclude the db files in the query of the bfwi created non in src tree gufi (for comparison)
 mwd=os.getcwd()
 os.chdir('%s' % ('fullafter'))
 # we also cant compare the totals in the directory record in the summary table because it will be off by that database file that
 # we removed with the query excluding those files - the totals are still in the summary record
 os.system('%s/%s/bfq -n1 -Pp -E "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from entries where name!=\'db.db\';" -S "select path(),name,type,inode,linkname,xattrs from vsummarydir;" -a -o %s/%s %s'  % (tdir,gdir,top,'fullbfqoutafternodbname','d0'))
 os.system('sort -o %s/%s.0 %s/%s.0' % (top,'fullbfqoutafternodbname',top,'fullbfqoutafternodbname'))
 os.chdir('%s' % (mwd))
 # get time when the full original gufi was made
 fo = open("fulltime", "rb")
 itt = fo.read(12);
 it = itt.rstrip()
 fo.close()
 # make a incremental gufi tree update and put it right in the source tree using bfwreaddirplus2db -b with suspect mode 3 dont need -c as it will use ctime of db
 #os.system('%s/%s/bfwreaddirplus2db -n 2 -A 3 -b -c %s -x d0'  % (tdir,gdir,it))
 os.system('%s/%s/bfwreaddirplus2db -n 2 -A 3 -b -x d0'  % (tdir,gdir))
 # do a bfq of the incrementally updated in src tree gufi again exclude totals from the summary record so it will match
 os.system('%s/%s/bfq -n1 -Pp -E "select path(),name,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattrs from entries where name!=\'db.db\';" -S "select path(),name,type,inode,linkname,xattrs from vsummarydir;" -a -o %s/%s %s'  % (tdir,gdir,top,'incrinsrc','d0'))
 os.system('sort -o %s/%s.0 %s/%s.0' % (top,'incrinsrc',top,'incrinsrc'))
 print "++++++++++ comparing bfq from in src tree gufi incremental  and bfq from from not in src gufi tree full "
 cmd=os.system('cmp %s/fullbfqoutafternodbname.0 %s/incrinsrc.0' % (top,top))
 exit_code = os.WEXITSTATUS(cmd)
 print "---------- test %s done result %d" % (testn,exit_code)
 if (exit_code!=0):
   exit(exit_code)
 #end ccbfwrdp2dbincrinsrc
################################### test  cbfwrdp2dbincrinsrc end ########################################

################################ functional tests and support start here ######################
#tester for number of lines in output files for comparison to norm for functional tests
def flines(fl,normlines,testnorm,exitnotnorm):
 outlines = sum(1 for line in open(fl))
 if (testnorm==1):
  if (outlines!=normlines):
   print "suspect: test out in %s lines %d normal lines %d" % (fl,outlines,normlines)
   if (exitnotnorm):
    exit(exitnotnorm)
  else:
   print "good: test out in %s lines %d normal lines %d" % (fl,outlines,normlines)
 return outlines
################################### func fbfq ############################################
def fbfq():
 testf='fbwq'
 normlines=1767
 testfull='%s/%s' % (funcout,testf)
 testn="functional bfq"
 cleanstart(testn,1)
 os.system('%s/runbfq %s/%s > %s' % (tdir,topgt,'d0',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fbfq end ############################################
################################### func fbfti ############################################
def fbfti():
 testf='fbwti'
 normlines=112
 testfull='%s/%s' % (funcout,testf)
 testn="functional bfti"
 cleanstart(testn,1)
 os.system('%s/%s/bfti -n 2 -s gt/d0/d00 > %s/bfti.0' % (tdir,gdir,top))
 os.system('%s/runbfti %s/%s > %s' % (tdir,topgt,'d0/d00',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fbfti end ############################################
################################### func fbfwi ############################################
def fbfwi():
 testf='fbwfi'
 normlines=306
 testfull='%s/%s' % (funcout,testf)
 testn="functional bfwi"
 cleanstart(testn,1)
 # make a place for the new gufi tree to be put
 os.system('mkdir fbfwidir')
 os.system('%s/runbfwi %s %s > %s' % (tdir,'d0','fbfwidir',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fbfwi end ############################################
################################### func fdfw ############################################
def fdfw():
 testf='fdwf'
 normlines=165
 testfull='%s/%s' % (funcout,testf)
 testn="functional dfw"
 cleanstart(testn,1)
 os.system('%s/rundfw %s > %s' % (tdir,'d0',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fdfw end ############################################
################################### func fquerydb ############################################
def fquerydb():
 testf='fquerydb'
 normlines=20
 testfull='%s/%s' % (funcout,testf)
 testn="functional querydb"
 cleanstart(testn,1)
 #add a tree index so querydb will list that table too
 os.system('%s/%s/bfti -n 2 -s gt/d0/d00 > %s/bfti.0' % (tdir,gdir,top))
 os.system('%s/runquerydb %s > %s' % (tdir,'gt/d0/d00',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fquerydb end ############################################
################################### func fbfwreaddirplus2db ############################################
def fbfwreaddirplus2db():
 testf='fbfwreaddirplus2db'
 normlines=6164
 testfull='%s/%s' % (funcout,testf)
 testn="functional readdirplus2db"
 cleanstart(testn,1)
 os.system('%s/runbfwreaddirplus2db %s > %s' % (tdir,'d0',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fquerydb end ############################################
################################### func fquerydbs ############################################
def fquerydbs():
 testf='fquerydbs'
 normlines=5
 testfull='%s/%s' % (funcout,testf)
 testn="functional querydbs"
 cleanstart(testn,1)
 os.system('%s/runquerydbs %s %s > %s' % (tdir,'gt/d0','2',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fquerydbs end ############################################
################################### func fgroupfilespacehog ############################################
def fgroupfilespacehog():
 testf='fgroupfilespacehog'
 normlines=4
 testfull='%s/%s' % (funcout,testf)
 testn="functional groupfilespacehog"
 cleanstart(testn,1)
 os.system('%s/groupfilespacehog %s %s > %s' % (sdir,'gt/d0','2',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fgroupfilespacehog end ############################################
################################### func fgroupfilespacehogusesummary ############################################
def fgroupfilespacehogusesummary():
 testf='fgroupfilespacehogusesummary'
 normlines=4
 testfull='%s/%s' % (funcout,testf)
 testn="functional groupfilespacehogusesummary"
 cleanstart(testn,1)
 #first we must add the user and group summary records to the summary tables
 os.system('%s/deluidgidsummaryrecs %s' % (sdir,'gt/d0'))
 os.system('%s/generategidsummary %s' % (sdir,'gt/d0'))
 os.system('%s/generateuidsummary %s' % (sdir,'gt/d0'))
 os.system('%s/groupfilespacehogusesummary %s %s > %s' % (sdir,'gt/d0','2',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fgroupfilespacehogusesummary end ############################################
################################### func fuserfilespacehog ############################################
def fuserfilespacehog():
 testf='fuserfilespacehog'
 normlines=4
 testfull='%s/%s' % (funcout,testf)
 testn="functional userfilespacehog"
 cleanstart(testn,1)
 os.system('%s/userfilespacehog %s %s > %s' % (sdir,'gt/d0','2',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fuserfilespacehog end ############################################
################################### func fuserfilespacehogusesummary ############################################
def fuserfilespacehogusesummary():
 testf='fuserfilespacehogusesummary'
 normlines=4
 testfull='%s/%s' % (funcout,testf)
 testn="functional userfilespacehogusesummary"
 cleanstart(testn,1)
 #first we must add the user and group summary records to the summary tables
 os.system('%s/deluidgidsummaryrecs %s' % (sdir,'gt/d0'))
 os.system('%s/generategidsummary %s' % (sdir,'gt/d0'))
 os.system('%s/generateuidsummary %s' % (sdir,'gt/d0'))
 os.system('%s/userfilespacehogusesummary %s %s > %s' % (sdir,'gt/d0','2',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fuserfilespacehogusesummary end ############################################
################################### func fuidgidsummary ############################################
def fuidgidsummary():
 testf='fuidgidsummary'
 normlines=324
 testfull='%s/%s' % (funcout,testf)
 testn="functional uidgidsummary"
 cleanstart(testn,1)
 #first we must add the user and group summary records to the summary tables
 os.system('%s/deluidgidsummaryrecs %s' % (sdir,'gt/d0'))
 os.system('%s/generategidsummary %s' % (sdir,'gt/d0'))
 os.system('%s/generateuidsummary %s' % (sdir,'gt/d0'))
 os.system('%s/%s/bfq -e 1 -S "select epath(),uid,totfiles,totsize from summary where rectype=1;" -Pp -n 1 %s > %s' % (tdir,gdir,'gt/d0',testfull))
 os.system('%s/%s/bfq -e 1 -S "select epath(),gid,totfiles,totsize from summary where rectype=2;" -Pp -n 1 %s >> %s' % (tdir,gdir,'gt/d0',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fuidgidummary end ############################################
################################### func foldbigfiles ##########################################
def foldbigfiles():
 testf='foldbigfiles'
 normlines=5
 testfull='%s/%s' % (funcout,testf)
 testn="functional oldbigfiles"
 cleanstart(testn,1)
 os.system('%s/oldbigfiles %s %s > %s' % (sdir,'gt/d0','2',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func foldbigfiles end ############################################
################################### func flistschemadb ##########################################
def flistschemadb():
 testf='flistschemadb'
 normlines=7
 testfull='%s/%s' % (funcout,testf)
 testn="functional listschemadb"
 cleanstart(testn,1)
 os.system('%s/listschemadb %s > %s' % (sdir,'gt/d0/db.db',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func flistschemadb end ############################################
################################### func flisttablesdb ##########################################
def flisttablesdb():
 testf='flisttablesdb'
 normlines=2
 testfull='%s/%s' % (funcout,testf)
 testn="functional listtablesdb"
 cleanstart(testn,1)
 os.system('%s/listtablesdb %s > %s' % (sdir,'gt/d0/db.db',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func flisttablesdb end ############################################
################################### func fgenuidgidsummaryavoidentriesscan ##########################################
def fgenuidgidsummaryavoidentriesscan():
 testf='fgenuidgidsummaryavoidentriesscan'
 normlines=167
 testfull='%s/%s' % (funcout,testf)
 testn="functional genuidgidsummaryavoidentriesscan"
 cleanstart(testn,1)
 os.system('%s/deluidgidsummaryrecs %s > %s' % (sdir,'gt/d0',testfull))
 os.system('%s/genuidsummaryavoidentriesscan %s >> %s' % (sdir,'gt/d0',testfull))
 os.system('%s/gengidsummaryavoidentriesscan %s >> %s' % (sdir,'gt/d0',testfull))
 os.system('%s/%s/bfq -e 1 -S "select epath(),uid,gid,totfiles,totsize from summary where rectype=0;" -Pp -n 1 %s >> %s' % (tdir,gdir,'gt/d0',testfull))
 os.system('%s/%s/bfq -e 1 -S "select epath(),uid,totfiles,totsize from summary where rectype=1;" -Pp -n 1 %s >> %s' % (tdir,gdir,'gt/d0',testfull))
 os.system('%s/%s/bfq -e 1 -S "select epath(),gid,totfiles,totsize from summary where rectype=2;" -Pp -n 1 %s >> %s' % (tdir,gdir,'gt/d0',testfull))
 outlines=flines(testfull,normlines,1,1)
################################### func fgenuidgidsummaryavoidentriesscan end ############################################

def runall():
 runallc()
 runallf()
 print "+_+_+_+_+_+_+_+_+_ end of all tests +_+_+_+_+_+_+_+_+"

def runallc():
 cincr3modf()
 cincr3deld()
 cincr3movd()
 cincr3addd()
 cincr3adddelmovdf()
 cincr2modf()
 cflatloaddfw()
 cflatloadfinddfw()
 cflatloadbfwi()
 cbfqbftiquerydbs()
 cbfwiinsrc()
 cbfwrp2dbfullinsrc()
 cbfwrdp2dbincrinsrc()
 print "+_+_+_+_+_+_+_+_+_ end of all correctness tests +_+_+_+_+_+_+_+_+"

def runallf():
 fbfq()
 fbfti()
 fbfwi()
 fdfw()
 fquerydb()
 fbfwreaddirplus2db()
 fquerydbs()
 fgroupfilespacehog()
 fgroupfilespacehogusesummary()
 fuserfilespacehog()
 fuserfilespacehogusesummary()
 fuidgidsummary()
 foldbigfiles()
 flistschemadb()
 flisttablesdb()
 fgenuidgidsummaryavoidentriesscan()
 print "+_+_+_+_+_+_+_+_+_ end of all functional tests +_+_+_+_+_+_+_+_+"

def usage():
 print "all                               - all correctness and functional tests"
 print "allc                              - all correctness tests"
 print "allf                              - all functional tests"
 print "cincr3modf                        - correctness incremental mode 3 mod file"
 print "cincr3deld                        - correctness incremental mode 3 del dir segment"
 print "cincr3movd                        - correctness incremental mode 3 mov dir segment"
 print "cincr3addd                        - correctness incremental mode 3 add dir segment"
 print "cincr3adddelmovdf                 - correctness incremental mode 3 add del mov dir segment and add file"
 print "cincr2modf                        - correctness incremental mode 2 mod file (use file suspect list file)"
 print "cflatloaddfw                      - correctness flat load using bfwi -u from dfw"
 print "cflatloadfinddfw                  - correctness flat load using bfwi -u from find dfw"
 print "cflatloadbfwi                     - correctness flat load using bfwi -u from bfwi"
 print "cbfqbftiquerydbs                  - correctness bfq and bfti and querydnb"
 print "cbfwiinsrc                        - correctness bfwi putting index in src tree"
 print "cbfwrp2dbfullinsrc                - correctness bfwreaddirplus2db full gufi tree build in src tree"
 print "cbfwrdp2dbincrinsrc               - correctness bfwreaddirplus2db incr gufi tree update in src tree"
 print "fbfq                              - functional bfq"
 print "fbfti                             - functional bfti"
 print "fbfwi                             - functional bfwi"
 print "fdfw                              - functional dfw"
 print "fquerydb                          - functional querydb"
 print "freaddirplus2db                   - functional readdirplus2db"
 print "fquerydbs                         - functional querydbs"
 print "fgroupfilespacehog                - functional groupfilespacehog"
 print "fgroupfilespacehogusesummary      - functional groupfilespacehogusesummary"
 print "fuserfilespacehog                 - functional userfilespacehog"
 print "fuserfilespacehogusesummary       - functional userfilespacehogusesummary"
 print "fuidgidsummary                    - functional uidgidsummary"
 print "foldbigfiles                      - functional oldbigfiles"
 print "flistschemadb                     - functional listschemadb"
 print "flisttablesdb                     - functional listtablesdb"
 print "fgenuidgidsummaryavoidentriesscan - functional genuidgidsummaryavoidentriesscan"

##################################  main  ############################################
if len(sys.argv) > 1:
 print "Primary argument is %s" % (sys.argv[1])
 if (sys.argv[1] == 'all'):
  runall()
  exit(0)
 elif (sys.argv[1] == 'allc'):
  runallc()
  exit(0)
 elif (sys.argv[1] == 'allf'):
  os.system('mkdir %s' % (funcout))
  runallf()
  exit(0)
 elif (sys.argv[1] == 'cincr3modf'):
  cincr3modf()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cincr3deld'):
  cincr3deld()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cincr3movd'):
  cincr3movd()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cincr3addd'):
  cincr3addd()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cincr3adddelmovdf'):
  cincr3adddelmovdf()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cincr2modf'):
  cincr2modf()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cflatloaddfw'):
  cflatloaddfw()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cflatloadfinddfw'):
  cflatloadfinddfw()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cflatloadbfwi'):
  cflatloadbfwi()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cbfqbftiquerydbs'):
  cbfqbftiquerydbs()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cbfwiinsrc'):
  cbfwiinsrc()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cbfwrp2dbfullinsrc'):
  cbfwrp2dbfullinsrc()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'cbfwrdp2dbincrinsrc'):
  cbfwrdp2dbincrinsrc()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fbfq'):
  os.system('mkdir %s' % (funcout))
  fbfq()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fbfti'):
  os.system('mkdir %s' % (funcout))
  fbfti()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fbfwi'):
  os.system('mkdir %s' % (funcout))
  fbfwi()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fdfw'):
  os.system('mkdir %s' % (funcout))
  fdfw()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fquerydb'):
  os.system('mkdir %s' % (funcout))
  fquerydb()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fbfwreaddirplus2db'):
  os.system('mkdir %s' % (funcout))
  fbfwreaddirplus2db()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fquerydbs'):
  os.system('mkdir %s' % (funcout))
  fquerydbs()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fgroupfilespacehog'):
  os.system('mkdir %s' % (funcout))
  fgroupfilespacehog()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fgroupfilespacehogusesummary'):
  os.system('mkdir %s' % (funcout))
  fgroupfilespacehogusesummary()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fuserfilespacehog'):
  os.system('mkdir %s' % (funcout))
  fuserfilespacehog()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fuserfilespacehogusesummary'):
  os.system('mkdir %s' % (funcout))
  fuserfilespacehogusesummary()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fuidgidsummary'):
  os.system('mkdir %s' % (funcout))
  fuidgidsummary()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'foldbigfiles'):
  os.system('mkdir %s' % (funcout))
  foldbigfiles()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'flistschemadb'):
  os.system('mkdir %s' % (funcout))
  flistschemadb()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'flisttablesdb'):
  os.system('mkdir %s' % (funcout))
  flisttablesdb()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 elif (sys.argv[1] == 'fgenuidgidsummaryavoidentriesscan'):
  os.system('mkdir %s' % (funcout))
  fgenuidgidsummaryavoidentriesscan()
  print "+_+_+_+_+_+_+_+_+_ end test %s +_+_+_+_+_+_+_+_+" % (sys.argv[1])
  exit(0)
 else:
  print "argument invalid %s" % (sys.argv[1])
  usage()
  exit(0)

else:
 print "argument required"
 usage()
 exit(1)
