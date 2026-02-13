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



delete from apps where appname='vrxpentriesall';
delete from appsqueries where appsqname='vrxpentriesall';
delete from apppart where apppartname='vrxpentriesall';
delete from appforminfo where appfname='vrxpentriesall';
delete from appformd where appdname='vrxpentriesall';
delete from appformdtree where appdtname='vrxpentriesall';

-- you may want to change 'http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
insert into apps values (NULL,'vrxpentriesall','files and directory summaries (entriesvar) (local) with Traversal Control','Query of local File Entries Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'vrxpentriesall',files and directory summaries (entriesvar) (remote) with Traversal Control','Query of Remote File Entries Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
*/

insert into appsqueries values (NULL,'vrxpentriesall','V','1',1);
-- these can be added to run the gufi_query remotely both command to run like ssh/pdsh and the remote host
/* these statements are used for running remote gufi
insert into appsqueries values (NULL,'vrxpentriesall','R','ssh',1);
insert into appsqueries values (NULL,'vrxpentriesall','A','REMOTEHOST',1);
*/
insert into appsqueries values (NULL,'vrxpentriesall','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrxpentriesall','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrxpentriesall','J','insert into aggregate select {interimselected} from intermediate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'vrxpentriesall','G','select {interimselected} from aggregate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'vrxpentriesall','E','insert into intermediate select {selectonly} from vrxpentries {where} {whereonly} {orderbyselected} {groupbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'vrxpentriesall','Z','select {displayselected} from vtmp {orderbyselected} {groupbyonly} {limitrec}',1);

insert into apppart values (NULL,'vrxpentriesall',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'vrxpentriesall',1,'treelist','Path Options','Optionally select search path','',1,2,0);
insert into apppart values (NULL,'vrxpentriesall',2,'input','File Input Criteria','Please enter at least one','',6,2,1);
insert into apppart values (NULL,'vrxpentriesall',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'vrxpentriesall',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'vrxpentriesall',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'vrxpentriesall',7,'limit','limit results','Optionally limit results','',1,2,0);
insert into apppart values (NULL,'vrxpentriesall',8,'downloadf','download results in csv','download results csv','',1,2,0);

insert into appforminfo values (NULL,'vrxpentriesall',1,'textlist','treelist','treelist','treelist','treelist','Search Local Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');

insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_path','rpath(sname,sroll) as path','path','path','Path','Path of File',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_name','name as name','name','name','File Name','Name of File',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_type','type as type','type','type','Entry Type','Entry type',1,1,0,'pattern=''[dfl]+''','placeholder=''d or f or l''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_nlink','nlink as nlink','nlink','nlink','number of links','number of links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_linkname','linkname as linkname','linkname','linkname','Link Name','Link Name of File',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_size.1','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_size.2','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'pattern=''[A-Za-z]+''','placeholder=''%str%''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_uid','uid as uid','uid','uid','File UID','File UID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_gid','gid as gid','gid','gid','File GID','File GID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_mtime.2','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_ctime.1','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_ctime.2','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_atime.1','atime as atime','atime','datetime(atime,"unixepoch") as atime','File Access Time','Access time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_atime.2','atime as atime','atime','datetime(atime,"unixepoch") as atime','File Access Time','Access time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_crtime.1','crtime as crtime','crtime','datetime(crtime,"unixepoch") as crtime','File Create Time','Create time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_crtime.2','crtime as crtime','crtime','datetime(crtime,"unixepoch") as crtime','File Create Time','Create time of File',1,1,1,'','','','','','','','','','','W');

insert into appforminfo values (NULL,'vrxpentriesall',4,'text','ENT_pathname','rpath(sname,sroll)||''/''||name as pathname','pathname','pathname','Pathname','Name of File',1,1,0,'required','checked','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'text','ENT_linkname','linkname as linkname','linkname','linkname','linkname','linkname',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_size','size as size','size','size','File Size','Size of File',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_inode','inode as inode','inode','inode','File Inode','Inode of File',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_nlink','nlink as nlink','nlink','nlink','File nlink','nlink of File',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_uid','uid as uid','uid','uidtouser(uid) as localuser','File UID','File UID',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_gid','gid as gid','gid','gidtogroup(gid) as localgroup','File GID','File GID',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_mtime','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_ctime','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_atime','atime as atime','atime','datetime(atime,"unixepoch") as atime','File Access Time','Access time of File',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','ENT_crtime','crtime as crtime','crtime','datetime(crtime,"unixepoch") as crtime','File Create Time','Create time of File',1,1,1,'','','','','','','','','','','S');

insert into appforminfo values (NULL,'vrxpentriesall',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');

insert into appforminfo values (NULL,'vrxpentriesall',5,'text','orderby2','orderby2','orderby2','orderby2','Second Order By','Second Order By',1,1,0,'','','','','','','','','','','O');
insert into appforminfo values (NULL,'vrxpentriesall',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');

insert into appforminfo values (NULL,'vrxpentriesall',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');

insert into appforminfo values (NULL,'vrxpentriesall',8,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');

insert into appformd values (NULL,'vrxpentriesall',5,'ENT_linkname','Linkname',1,0,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_pathname','Pathname',1,0,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_size','File Size',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_mode','File Mode',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_inode','File inode',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_nlink','File nlink',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_uid','File uid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_gid','File gid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_type','Entry type',1,0,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_xattr_name','Xattr Name',1,0,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_xattr_value','Xattr Value',1,0,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_mtime','File Mtime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_ctime','File Ctime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_atime','File Atime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_crtime','File CRtime',1,0,'int64');

insert into appformd values (NULL,'vrxpentriesall',5,'ENT_linkname','Linkname',1,1,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_pathname','Pathname',1,1,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_size','File Size',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_mode','File Mode',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_inode','File inode',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_nlink','File nlink',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_uid','File uid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_gid','File gid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_type','Entry type',1,1,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_xattr_name','Xattr Name',1,1,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_xattr_value','Xattr Value',1,1,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_mtime','File Mtime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_ctime','File Ctime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_atime','File Atime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'ENT_crtime','File CRtime',1,1,'int64');

-- you may want to put in your own path list for LOCALSEARCHSUBPATHn  you can have as many as you like
insert into appformdtree values (NULL,'vrxpentriesall',1,'LOCALSEARCHSUBPATH1','');
insert into appformdtree values (NULL,'vrxpentriesall',1,'LOCALSEARCHSUBPATH2','');
