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
insert into apps values (NULL,'vrxpentriesall','files and directory summaries (allvar) (local) with Traversal Control','Query of local File Entries Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'vrxpentriesall',files and directory summaries (allvar) (remote) with Traversal Control','Query of Remote File Entries Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
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
insert into apppart values (NULL,'vrxpentriesall',3,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
insert into apppart values (NULL,'vrxpentriesall',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'vrxpentriesall',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'vrxpentriesall',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'vrxpentriesall',7,'limit','limit results','Optionally limit results','',1,2,0);
insert into apppart values (NULL,'vrxpentriesall',8,'downloadf','download results in csv','download results csv','',1,2,0);

insert into appforminfo values (NULL,'vrxpentriesall',1,'textlist','treelist','treelist','treelist','treelist','Search Local Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');

insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_path','rpath(sname,sroll) as path','path','path','Path','Path of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_name','name as name','name','name','File Name','Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_type','type as type','type','type','Entry Type','Entry type',1,1,0,'pattern=''[dfl]+''','placeholder=''d or f or l''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_nlink','nlink as nlink','nlink','nlink','number of links','number of links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_linkname','linkname as linkname','linkname','linkname','Link Name','Link Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_size.1','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'int64','ENT_size.2','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'pattern=''[A-Za-z0-9]+''','placeholder=''%str%''','','','','','','','','','W');
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

insert into appforminfo values (NULL,'vrxpentriesall',3,'text','DIR_name','dname as dname','dname','dname','Directory Name','Name of Directory',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dnlink','dnlink as dnlink','dnlink','dnlink','number of dlinks','number of dlinks',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_duid','duid as duid','duid','duid','Directory UID','Directory UID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dgid','dgid as dgid','dgid','dgid','Directory GID','Directory GID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dsize.1','dsize as dsize','dsize','dsize','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dsize.2','dsize as dsize','dsize','dsize','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmtime.1','dmtime as dmtime','dmtime','datetime(dmtime,"unixepoch") as dmtime','Directory Mod Time','Mod time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmtime.2','dmtime as dmtime','dmtime','datetime(dmtime,"unixepoch") as dmtime','Directory Mod Time','Mod time of Directory ',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dctime.1','dctime as dctime','dctime','datetime(dctime,"unixepoch") as dctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dctime.2','dctime as dctime','dctime','datetime(dctime,"unixepoch") as dctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_datime.1','datime as datime','datime','datetime(datime,"unixepoch") as datime','Directory Access Time','Access time of Direcctory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_datime.2','datime as datime','datime','datetime(datime,"unixepoch") as datime','Direcctory Access Time','Access time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dminctime','dminctime as dminctime','dminctime','datetime(dminctime,"unixepoch") as dminctime','Min file ctime','Min file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmaxctime','dmaxctime as dmaxctime','dmaxctime','datetime(dmaxctime,"unixepoch") as dmaxctime','Max file ctime','Max file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dminmtime','dminmtime as dminmtime','dminmtime','datetime(dminmtime,"unixepoch") as dminmtime','Min file mtime','Min file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmaxmtime','dmaxmtime as dmaxmtime','dmaxmtime','datetime(dmaxmtime,"unixepoch") as dmaxmtime','Max file mtime','Max file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dminatimei','dminatime as dminatime','dminatime','datetime(dminatime,"unixepoch") as dminatime','Min file atime','Min file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmaxatime','dmaxatime as dmaxatime','dmaxatime','datetime(dmaxatime,"unixepoch") as dmaxatime','Max file atime','Max file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmincrtime','dmincrtime as dmincrtime','dmincrtime','datetime(dmincrtime,"unixepoch") as dmincrtime','Min file crtime','Min file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmaxcrtime','dmaxcrtime as dmaxcrtime','dmaxcrtime','datetime(dmaxcrtime,"unixepoch") as dmaxcrtime','Max file crtime','Max file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',2,'text','DIR_dlinkname','dlinkname as dlinkname','dlinkname','dlinkname','DLink Name','DLink Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_ddepth','ddepth as ddepth','ddepth','ddepth','Directory depth','Directory depth',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotxattr','dtotxattr as dtotxattr','dtotxattr','dtotxattr','Directory dtotxattr','Directory dtotxattr',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmode','dmode as dmode','dmode','modetotxt(dmode) as dmode','Directory Mode','Mode of Directory',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotfiles','dtotfiles as dtotfiles','dtotfiles','dtotfiles','Directory total files','Directory total files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotlinks','dtotlinks as dtotlinks','dtotlinks','dtotlinks','Directory total links','Directory total links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dminsize','dminsize as dminsize','dminsize','dminsize','Directory min filesize','Directory min filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmaxsize','dmaxsize as dmaxsize','dmaxsize','dmaxsize','Directory max filesize','Directory max filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotzero','dtotzero as dtotzero','dtotzeroe','dtotzero','Directory total zero files','Directory total zero files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dminuid','dminuid as dminuid','dminuid','dminuid','Directory min uid','Directory min uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmaxuid','dmaxuid as dmaxuid','dmaxuid','dmaxuid','Directory max uid','Directory max uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmingid','dmingid as dmingid','dmingid','dmingid','Directory min gid','Directory min gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dmaxgid','dmaxgid as dmaxgid','dmaxgid','dmaxgid','Directory max gid','Directory max gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotltk','dtotltk as dtotltk','dtotltk','dtotltk','Directory total files ltk','Directory total files ltk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotmtk','dtotmtk as dtotmtk','dtotmtk','dtotmtk','Directory total files mtk','Directory total files mtk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotltm','dtotltm as dtotltm','dtotltm','dtotltm','Directory total files ltm','Directory total files ltm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotmtm','dtotmtm as dtotmtm','dtotmtm','dtotmtm','Directory total files mtm','Directory total files mtm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotmtg','dtotmtg as dtotmtg','dtotmtg','dtotmtg','Directory total files mtg','Directory total files mtg',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotmtt','dtotmtt as dtotmtt','dtotmtt','dtotmtt','Directory total files mtt','Directory total files mtt',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesall',3,'int64','DIR_dtotsize','dtotsize as dtotsize','dtotsize','dtotsize','Directory total files size','Directory total files size',1,1,0,'min=''0''','','','','','','','','','','W');

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

insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmode','dmode as dmode','dmode','modetotxt(dmode) as dmode','File DMode','DMode of File',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dnlink','dnlink as dnlink','dnlink','dnlink','Dir nlink','nlink of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_duid','duid as duid','duid','duid','Dir uid','uid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dgid','dgid as dgid','dgid','dgid','Dir gid','gid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dsize','dsize as dsize','dsize','dsize','Dir size','size of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmtime','dmtime as dmtime','dmtime','datetime(dmtime,"unixepoch") as dmtime','Dir Mod Time','Mod time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dctime','dctime as dctime','dctime','datetime(dctime,"unixepoch") as dctime','Dir Change Time','Change time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_datime','datime as datime','datime','datetime(datime,"unixepoch") as datime','Dir Access Time','Access time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'text','DIR_dlinkname','dlinkname as dlinkname','dlinkname','dlinkname','dlinkname','dlinkname',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotfile','dtotfile as dtotfile','dtotfile','dtotfile','Dir totfile ','totfile of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotlinks','dtotlinks as dtotlinks','dtotlinks','dtotlinks','Dir totlinks ','totlinks of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dminuid','dminuid as dminuid','dminuid','dminuid','Dir minuid','minuid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmaxuid','dmaxuid as dmaxuid','dmaxuid','dmaxuid','Dir maxuid','maxuid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmingid','dmingid as dmingid','dmingid','dmingid','Dir mingid','mingid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmaxgid','dmaxgid as dmaxgid','dmaxgid','dmaxgid','Dir maxgid','maxgid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dminsize','dminsize as dminsize','dminsize','dminsize','Dir minsize','minsize of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmaxsize','dmaxsize as dmaxsize','dmaxsize','dmaxsize','Dir maxsize','maxsize of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotzero','dtotzero as dtotzero','dtotzero','dtotzero','Dir totzero','totzero of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotltk','dtotltk as dtotltk','dtotltk','dtotltk','Dir totltk','totltk of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotmtk','dtotmtk as dtotmtk','dtotmtk','dtotmtk','Dir totmtk','totmtk of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotltm','dtotltm as dtotltm','dtotltm','dtotltm','Dir totltm','totltm of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotmtm','dtotmtm as dtotmtm','dtotmtm','dtotmtm','Dir totmtm','totmtm of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotmtg','dtotmtg as dtotmtg','dtotmtg','dtotmtg','Dir totmtg','totmtg of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotmtt','dtotmtt as dtotmtt','dtotmtt','dtotmtg','Dir totmtt','totmtt of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotsize','dtotsize as dtotsize','dtotsize','dtotsize','Dir totsize','totsize of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dminctime','dminctime as dminctime','dminctime','datetime(dminctime,"unixepoch") as dminctime','Dir minctime','minctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmaxctime','dmaxctime as dmaxctime','dmaxctime','datetime(dmaxctime,"unixepoch") as dmaxctime','Dir maxctime','maxctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dminmtime','dminmtime as dminmtime','dminmtime','datetime(dminmtime,"unixepoch") as dminmtime','Dir minmtime','minctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmaxmtime','dmaxmtime as dmaxmtime','dmaxmtime','datetime(dmaxmtime,"unixepoch") as dmaxmtime','Dir maxmtime','maxctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dminatime','dminatime as dminatime','dminatime','datetime(dminatime,"unixepoch") as dminatime','Dir minatime','minatime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmaxatime','dmaxatime as dmaxatime','dmaxatime','datetime(dmaxatime,"unixepoch") as dmaxatime','Dir maxatime','maxatime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmincrtime','dmincrtime as dmincrtime','dmincrtime','datetime(dmincrtime,"unixepoch") as dmincrtime','Dir mincrtime','mincrtime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dmaxcrtime','dmaxcrtime as dmaxcrtime','dmaxcrtime','datetime(dmaxcrtime,"unixepoch") as dmaxcrtime','Dir maxcrtime','maxcrtime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesall',4,'int64','DIR_dtotxattr','dtotxattr as dtotxattr','dtotxattr','dtotxattr','Dir totxattr','totxattr of Dir',1,1,0,'','','','','','','','','','','S');

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
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmode','Dir DMode',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dnlink','Dir Dnlink',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_duid','Dir Duid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dgid','Dir Dgid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dsize','Dir Dsize',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmtime','Dir Dmtime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dctime','Dir Dctime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_datime','Dir Datime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dlinkname','Dir Dlinkname',1,0,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotfile','Dir Dtotfile',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotlinks','Dir Dtotlinks',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminuid','Dir Dminuid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxuid','Dir Dmaxuid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmingid','Dir Dmingid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxgid','Dir Dmaxgid',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminsize','Dir Dminsize',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxsize','Dir Dmaxsize',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotzero','Dir Dtotzero',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotltk','Dir Dtotltk',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtk','Dir Dtotmtk',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotltm','Dir Dtotltm',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtm','Dir Dtotmtm',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtg','Dir Dtotmtg',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtt','Dir Dtotmtt',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotsize','Dir Dtotsize',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminctime','Dir Dminctime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxctime','Dir Dmaxctime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminmtime','Dir Dminmtime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxmtime','Dir Dmaxmtime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminatime','Dir Dminatime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxatime','Dir Dmaxatime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmincrtime','Dir Dmincrtime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxcrtime','Dir Dmaxcrtime',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotxattr','Dir Dtotxattr',1,0,'int64');

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
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmode','Dir DMode',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dnlink','Dir Dnlink',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_duid','Dir Duid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dgid','Dir Dgid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dsize','Dir Dsize',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmtime','Dir Dmtime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dctime','Dir Dctime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_datime','Dir Datime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dlinkname','Dir Dlinkname',1,1,'text');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotfile','Dir Dtotfile',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotlinks','Dir Dtotlinks',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminuid','Dir Dminuid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxuid','Dir Dmaxuid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmingid','Dir Dmingid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxgid','Dir Dmaxgid',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminsize','Dir Dminsize',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxsize','Dir Dmaxsize',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotzero','Dir Dtotzero',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotltk','Dir Dtotltk',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtk','Dir Dtotmtk',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotltm','Dir Dtotltm',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtm','Dir Dtotmtm',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtg','Dir Dtotmtg',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotmtt','Dir Dtotmtt',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotsize','Dir Dtotsize',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminctime','Dir Dminctime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxctime','Dir Dmaxctime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminmtime','Dir Dminmtime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxmtime','Dir Dmaxmtime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dminatime','Dir Dminatime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxatime','Dir Dmaxatime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmincrtime','Dir Dmincrtime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dmaxcrtime','Dir Dmaxcrtime',1,1,'int64');
insert into appformd values (NULL,'vrxpentriesall',5,'DIR_dtotxattr','Dir Dtotxattr',1,1,'int64');

-- you may want to put in your own path list for LOCALSEARCHSUBPATHn  you can have as many as you like
insert into appformdtree values (NULL,'vrxpentriesall',1,'LOCALSEARCHSUBPATH1','');
insert into appformdtree values (NULL,'vrxpentriesall',1,'LOCALSEARCHSUBPATH2','');
