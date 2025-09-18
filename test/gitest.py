#!/usr/bin/python3
# This file is part of GUFI, which is part of MarFS, which is released
# under the BSD license.
#
#
# Copyright (c) 2017, Los Alamos National Security (LANS), LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#
# From Los Alamos National Security, LLC:
# LA-CC-15-039
#
# Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
# Copyright 2017. Los Alamos National Security, LLC. This software was produced
# under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
# Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
# the U.S. Department of Energy. The U.S. Government has rights to use,
# reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
# ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
# ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
# modified to produce derivative works, such modified software should be
# clearly marked, so as not to confuse it with the version available from
# LANL.
#
# THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
# OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE.



import subprocess
import os
import time
import sys

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

def make_source_tree(root):
    print("make_source_tree: ", root)
    # clean up and make initial directories
    os.system('rm -rf %s' % (root))
    os.system('mkdir %s' % (root))
    os.system('mkdir %s/d0' % (root))
    os.system('mkdir %s/gt' % (root))
    # make the source tree
    for i in dirs0:
        os.system('mkdir %s/d0/d0%d' % (root,i))
        for j in files:
           os.system('touch %s/d0/d0%d/f0%d%d' % (root,i,i,j))
        for k in dirs1:
             os.system('mkdir %s/d0/d0%d/d0%d%d' % (root,i,i,k))
             for l in files:
                 os.system('touch %s/d0/d0%d/d0%d%d/f0%d%d%d' % (root,i,i,k,i,k,l))
             for m in dirs2:
                 os.system('mkdir %s/d0/d0%d/d0%d%d/d0%d%d%d' % (root,i,i,k,i,k,m))
                 for n in files:
                     os.system('touch %s/d0/d0%d/d0%d%d/d0%d%d%d/f0%d%d%d%d' % (root,i,i,k,i,k,m,i,k,m,n))


def do_gufi_query_command(entries_table, output_file, gufi_index):
    entries_sql = """
SELECT Path(),NAME,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattr_names
FROM   %s;
    """ % entries_table

    summary_sql = """
SELECT Path(),NAME,type,inode,nlink,size,mode,uid,gid,blksize,blocks,mtime,ctime,linkname,xattr_names
FROM   vsummarydir;
    """

    os.system('gufi_query -n1 -x -d\'|\' -E "%s" -S "%s" -a 1 -o "%s" "%s"'
               % (entries_sql, summary_sql, output_file, gufi_index))


# initialize a src tree, make a full gufi tree, do a bfq in case we want to compare with the full at initial state, make a full initial tree snap
def ginit():
    print ("initializing a tree, full gufi tree, and full tree snap")
    make_source_tree(top)

    # wait a second and then put time in the current timestamp file - about to start the full
    time.sleep(1)
    #we have to do most of this from the top directory but sometimes we have to cd to the top of the gufi tree for bfqs
    # to have the same relative diretory structure
    os.chdir(top)
    mytime=str(time.mktime(time.localtime()))
    mysec=mytime.partition(".")
    os.system('echo %s > fulltime' % (mysec[0]))

    # create a gufi tree
    os.system('gufi_dir2index -n 2 -x d0 gt')

    # this is just for testing - make a bfq output to compare with if we want to - leave out atime as it may change
    os.chdir(topgt)
    do_gufi_query_command(entries_table = "entries", output_file = "%s/fullbfqout" % top, gufi_index = "%s/d0" % topgt)

    summary_sql = """
INSERT INTO readdirplus
SELECT Path(),type,inode,pinode,0
FROM   vsummarydir;
    """
    initial_sql = """
CREATE TABLE readdirplus
  (
     path TEXT,type TEXT,inode INT64 PRIMARY KEY,pinode INT64,suspect INT64
  );
    """
    # make a directory structure snapshot database from the full
    #i think this just needs to be path not path name as its the summary table and last part of path is the name
    os.system('gufi_query -n2 -S "%s" -I "%s" -O %s/fullinitsnapdb d0'  % (summary_sql, initial_sql, top))

    os.chdir(top)

    # what have we accomplished
    print ("we should have created a source tree d0, a gufi tree gt, a full gufi_query output /tmp/giteset/fullbfqout0 to compare to, a full initial snap of the tree structure /tmp/gitest/fulinitsnapdb.0 and .1 (two threads), and a time stamp /tmp/gitest/fulltime")

    print ("time after create initial gufi tree sec %s" % (mysec[0]))

# this is where incremental of a gufi is done in addition we do a gufi_query of the incrementally updated gufi tree for comparison
def gincr(suspectopt):
    mytime=str(time.mktime(time.localtime()))
    mysec=mytime.partition(".")
    os.system('echo %s > incrtime' % (mysec[0]))
    # get the full time so we can use it to generate new dir snap and suspects
    fo = open("fulltime", "r")
    itt = fo.read(12);
    it = itt.rstrip()
    fo.close()
    os.system('mkdir %s' % (toptg))
    os.system('bfwreaddirplus2db -R -n 2 %s -O incrsnapdb -c %s -x d0 %s'  % (suspectopt,it,toptg))

    # now that we have initial and incremental snaps we need to do a full outer join to diff these, we need to calculate depths for operation ordering, and we need to create a permanent diff table so we can do several queries against it efficiently
    # attach full and incr dbs one per thread
    diff_db_sql_setup = """
-- attach full and incr dbs one per thread
attach 'fullinitsnapdb.0' AS full0;
attach 'fullinitsnapdb.1' AS full1;
attach 'incrsnapdb.0' AS incr0;
attach 'incrsnapdb.1' AS incr1;
"""
    # create temp views of all fulls and all incrs via union
    diff_db_sql_create_views = """
CREATE TEMP VIEW b0 AS
SELECT *
FROM full0.readdirplus
UNION ALL
SELECT *
FROM full1.readdirplus;

CREATE TEMP VIEW b1 AS
SELECT *
FROM incr0.readdirplus
UNION ALL
SELECT *
FROM incr1.readdirplus;
    """
    diff_db_sql_join = """
CREATE TEMP VIEW jt AS
SELECT a0.path a0path,
       a0.type a0type,
       a0.inode a0inode,
       a0.pinode a0pinode,
       a0.suspect a0suspect,
       a1.path a1path,
       a1.type a1type,
       a1.inode a1inode,
       a1.pinode a1pinode,
       a1.suspect a1suspect
FROM b0 AS a0
LEFT OUTER JOIN b1 AS a1 ON a0.inode=a1.inode
UNION ALL
SELECT a0.path a0path,
       a0.type a0type,
       a0.inode a0inode,
       a0.pinode a0pinode,
       a0.suspect a0suspect,
       a1.path a1path,
       a1.type a1type,
       a1.inode a1inode,
       a1.pinode a1pinode,
       a1.suspect a1suspect
FROM b1 AS a1
LEFT OUTER JOIN b0 AS a0 ON a1.inode=a0.inode
WHERE a0.inode IS NULL;
    """
    # create the output diff table
    diff_db_sql_output = """
CREATE TABLE diff
  (
     a0path TEXT,a0type TEXT,a0inode INT(64),a0pinode INT(64),a0suspect INT(64),
     a1path TEXT,a1type TEXT,a1inode INT64,a1pinode INT(64),a1suspect INT(64),
     a0depth INT(64),a1depth INT(64)
  );
    """
    diff_db_sql_final = """
INSERT INTO diff
SELECT *,
       (length(a0path)-length(replace(a0path, '/', '')))/1 AS a0depth,
       (length(a1path)-length(replace(a1path, '/', '')))/1 AS a1depth
FROM jt
WHERE a0pinode!=a1pinode
  OR a0path!=a1path
  OR a0inode IS NULL
  OR a1inode IS NULL
  OR a1suspect=1;
    """
    fd = open("incrdiff", "a")
    fd.write(diff_db_sql_setup)
    fd.write(diff_db_sql_create_views)
    fd.write(diff_db_sql_join)
    fd.write(diff_db_sql_output)
    fd.write(diff_db_sql_final)
    fd.close()
    #build the diff db
    os.system('sqlite3 diffdb \'.read incrdiff\'')

    # this allows you to start from the bottom of the current gufi tree and do deletes of directories and moves of directories to a parking lot
    print ("delete and move reverse order")
    diff_db_sql = """
SELECT a0path,
       a1path,
       a1inode,
       CASE WHEN a1path IS NULL THEN 1 ELSE 0 END
FROM diff
WHERE a1path IS NULL
  OR a0pinode!=a1pinode
  OR a0path!=a1path
ORDER BY a0depth DESC;
    """
    proc = subprocess.Popen('sqlite3 diffdb \'%s\'' % (diff_db_sql), stdout=subprocess.PIPE, shell=True, text=True)
    for line in proc.stdout:
        a0path=line.split('|')[0]
        a1path=line.split('|')[1]
        a1inode=line.split('|')[2]
        delr=line.split('|')[3]
        mdel=delr.rstrip()
        my_dir="%s/%s" % (topgt,a0path)
        my_lot="%s/d.%s" % (toptg,a1inode)
        # if delete remove with recursion this dir
        if mdel=='1':
            #del
            print ("deleteing subdir %s" % (my_dir))
            os.system('rm -rf %s' % (my_dir))
        else:
            #move to lot
            print ("moving %s to  %s" % (my_dir,my_lot))
            os.system('mv %s %s' % (my_dir,my_lot))
    proc.wait()
    print ("delete and move reverse order done")

    # this allows you to start from the top and add new directories and move directories from the parking lot back into the gufi tree
    print ("new and move in order")
    diff_db_sql = """
SELECT a0path,
       a1path,
       a1inode,
       CASE WHEN a0path IS NULL THEN 1 ELSE 0 END
FROM diff
WHERE a0path IS NULL
  OR a0pinode!=a1pinode
  OR a0path!=a1path
ORDER BY a1depth ASC;
    """
    proc = subprocess.Popen('sqlite3 diffdb \'%s\'' % (diff_db_sql), stdout=subprocess.PIPE, shell=True, text=True)
    for line in proc.stdout:
        a0path=line.split('|')[0]
        a1path=line.split('|')[1]
        a1inode=line.split('|')[2]
        newr=line.split('|')[3]
        mnew=newr.strip()
        my_dir="%s/%s" % (topgt,a1path)
        my_lot="%s/d.%s" % (toptg,a1inode)
        # if new add the directory
        if mnew=='1':
            #add new dir assume we will fix the permissions later when we populate it
            print ("mkdir %s" % (my_dir))
            os.system('mkdir %s' % (my_dir))
        else:
            #move from lot
            print ("moving back %s to  %s" % (my_lot,my_dir))
            os.system('mv %s %s' % (my_lot,my_dir))
    proc.wait()
    print ("new and move in order done")

    # this tells you what gufi dbs are in the temp area from bfwreaddirplus2db walk that need to be put into the gufi tree/replace/new and potentially
    # update the name of the dir, the mode/owner/group of the dir
    print ("update")
    diff_db_sql = """
SELECT a0path,
       a1path,
       CASE WHEN a0path=a1path THEN 0 ELSE 1 END,
       a1inode,
       a1pinode,
       CASE WHEN a0pinode=a1pinode THEN 0 ELSE 1 END
FROM diff
WHERE a1path IS NOT NULL
ORDER BY a1depth ASC;
    """
    proc = subprocess.Popen('sqlite3 diffdb \'%s\'' % (diff_db_sql), stdout=subprocess.PIPE, shell=True, text=True)
    for line in proc.stdout:
        #the real code does filtering here
        a0path=line.split('|')[0]
        a1path=line.split('|')[1]
        rename=line.split('|')[2]
        a1inode=line.split('|')[3]
        a1pinode=line.split('|')[4]
        mover=line.split('|')[5]
        move=mover.rstrip()
        my_file="%s/%s" % (toptg,a1inode)
        exists=os.path.isfile(my_file)
        if exists:
            sqlstmt="sqlite3 %s \'select mode, uid, gid from vsummarydir;\'" % (my_file)
            pproc = subprocess.Popen(sqlstmt,stdout=subprocess.PIPE,shell=True,text=True)
            for pline in pproc.stdout:
                 a1mode=pline.split('|')[0]
                 a1uid=pline.split('|')[1]
                 a1gidr=pline.split('|')[2]
                 a1gid=a1gidr.rstrip()
                 # we copy/move the gufi into this dir
                 topath='%s/%s' % (thegt,a1path)
                 # we set the mode, uid and gid
                 # go through the mode bits conversion
                 # we copy/move the gufi into this dir
                 i1mode=int(a1mode)
                 o1mode=oct(i1mode & 0o777)[-3:]
                 c1mode='0o%s' % o1mode
                 mfrom='%s/%s' % (toptg,a1inode)
                 mto='%s/db.db' % (topath)
                 print ("chmod chown of %s mode %s uid %s gid %s move %s to %s" % (topath,c1mode,a1uid,a1gid,mfrom,mto))
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
            print ("file %s not found - probably just a move a0path %s a1path %s" % (my_file,a0path,a1path))
    proc.wait()
    print ("update done")
    os.chdir(topgt)
    # leave out atime
    do_gufi_query_command(entries_table = "entries", output_file = "%s/incrbfqoutafter" % top, gufi_index = "d0")
    os.chdir(top)

# make a full gufi tree of the src tree after mods then do a bfq of that for comparison with an incrementally updated gufi tree
def gfullafter():
    os.system('mkdir %s' % (topfullafter))
    os.system('gufi_dir2index -n 2 -x d0 %s' % (fa))
    os.chdir(topfullafter)
    #leave out atime
    do_gufi_query_command(entries_table = "vrpentries", output_file = "%s/fullbfqoutafter" % top, gufi_index = "d0")
    os.chdir(top)

def test_prep(name):
    os.chdir(mtmp)
    ginit()
    os.chdir(top)
    time.sleep(1)
    print ("\n---------- test %s start" % (name))

def test_finish(name):
    print ("++++++++++ comparing bfq from fullafter and bfq from incrafter")
    cmd = os.system('cmp %s/fullbfqoutafter.0 %s/incrbfqoutafter.0' % (top, top))
    exit_code = os.WEXITSTATUS(cmd)
    print ("---------- test %s done result %d" % (name, exit_code))

# Runs a test with the given name, and calls the function operation()
# to do the test-specific changes.
def do_test(name, operation):
    test_prep(name)

    operation()

    # do a full gufi tree and a bfq on it to compare with the gufi we did an incremental on
    gfullafter()
    # wait until incremental time
    time.sleep(1)
    # do incremental update of gufi tree
    gincr("-A 3")

    test_finish(name)

def test_1():
    os.system('echo cheese >> d0/d00/d000/f0000')

def test_2():
    os.system('rm -rf d0/d00/d000')

def test_3():
    os.system('mv d0/d00/d000 d0')

def test_4():
    os.system('mkdir d0/d00/nd000')
    os.system('touch d0/d00/nd000/fnd000')
    os.system('mkdir d0/d00/nd000/nd0000')
    os.system('touch d0/d00/nd000/nd0000/fnd0000')

def test_5():
    os.system('mkdir d0/d00/nd000')
    os.system('touch d0/d00/nd000/fnd000')
    os.system('mkdir d0/d00/nd000/nd0000')
    os.system('touch d0/d00/nd000/nd0000/fnd0000')
    os.system('mv d0/d00/d000 d0')
    os.system('mv d0/d000/d0000 d0')
    os.system('mv d0/d000 d0/d0000')
    os.system('rm -rf d0/d01/d000')
    os.system('echo cheese >> d0/d02/d020/f0200')

def test_6():
    testn="change an existing file using suspect file for file suspects"
    test_prep(testn)

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
    gincr("-A 2 -W %s/suspects " % (top))

    test_finish(testn)

if __name__ == "__main__":
    do_test("change contents of one file", test_1)
    do_test("delete directory segment", test_2)
    do_test("move a directory segment", test_3)
    do_test("add a directory segment", test_4)
    do_test("add, delete, move multiple segments and touch an existing file", test_5)

    # test 6 has a different structure than the other 5 so it doesn't use do_test()
    test_6()
