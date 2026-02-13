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



delete from apps where appname='gufibigentriesremotetraversal';
delete from appsqueries where appsqname='gufibigentriesremotetraversal';
delete from apppart where apppartname='gufibigentriesremotetraversal';
delete from appforminfo where appfname='gufibigentriesremotetraversal';
delete from appformd where appdname='gufibigentriesremotetraversal';
delete from appformdtree where appdtname='gufibigentriesremotetraversal';
insert into apps values (NULL,'gufibigentriesremotetraversal','Remote File Entries Query with Traversal Control','Query of Remote File Entries Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','V','1',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','R','ssh',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','A','REMOTEHOST',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','J','insert into aggregate select {interimselected} from intermediate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','G','select {interimselected} from aggregate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','E','insert into intermediate select {selectonly} from vrxpentries {where} {whereonly} {orderbyselected} {groupbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'gufibigentriesremotetraversal','Z','select {displayselected} from vtmp {orderbyselected} {groupbyonly} {limitrec}',1);
insert into apppart values (NULL,'gufibigentriesremotetraversal',0,'travcontrol','Remote Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'gufibigentriesremotetraversal',1,'treelist','Path Options','Optionally select search path','',1,2,0);
insert into apppart values (NULL,'gufibigentriesremotetraversal',2,'input','File Input Criteria','Please enter at least one','',6,2,1);
insert into apppart values (NULL,'gufibigentriesremotetraversal',3,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
insert into apppart values (NULL,'gufibigentriesremotetraversal',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'gufibigentriesremotetraversal',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'gufibigentriesremotetraversal',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'gufibigentriesremotetraversal',7,'limit','limit results','Optionally limit results','',1,2,0);
insert into apppart values (NULL,'gufibigentriesremotetraversal',8,'downloadf','download results in csv','download results csv','',1,2,0);
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',1,'textlist','treelist','treelist','treelist','treelist','Search Remote Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'text','ENT_path','rpath(sname,sroll) as path','path','path','Path','Path of File',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'text','ENT_name','name as name','name','name','File Name','Name of File',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'pattern=''[A-Za-z0-9,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_size.1','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_size.2','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'pattern=''[A-Za-z0-9]+''','placeholder=''%str%''','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_uid','uid as uid','uid','uid','File UID','File UID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_gid','gid as gid','gid','gid','File GID','File GID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_mtime.2','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_ctime.1','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',2,'int64','ENT_ctime.2','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',3,'int64','DIR_dmode','dmode as dmode','dmode','modetotxt(dmode) as dmode','Directory Mode','Mode of Directory',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',3,'int64','DIR_dtotfiles','dtotfiles as dtotfiles','dtotfiles','dtotfiles','Directory total files','Directory total files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',3,'int64','DIR_dtotlinks','dtotlinks as dtotlinks','dtotlinks','dtotlinks','Directory total links','Directory total links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',3,'int64','DIR_dminsize','dminsize as dminsize','dminsize','dminsize','Directory min filesize','Directory min filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',3,'int64','DIR_dmaxsize','dmaxsize as dmaxsize','dmaxsize','dmaxsize','Directory max filesize','Directory max filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',3,'int64','DIR_dtotzero','dtotzero as dtotzero','dtotzeroe','dtotzero','Directory total zero files','Directory total zero files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'text','ENT_pathname','rpath(sname,sroll)||''/''||name as pathname','pathname','pathname','Pathname','Name of File',1,1,0,'required','checked','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'int64','ENT_size','size as size','size','size','File Size','Size of File',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'int64','ENT_uid','uid as uid','uid','uidtouser(uid) as localuser','File UID','File UID',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'int64','ENT_gid','gid as gid','gid','gidtogroup(gid) as localgroup','File GID','File GID',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'int64','ENT_mtime','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',4,'int64','ENT_ctime','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',5,'text','orderby2','orderby2','orderby2','orderby2','Second Order By','Second Order By',1,1,0,'','','','','','','','','','','O');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');
insert into appforminfo values (NULL,'gufibigentriesremotetraversal',8,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_pathname','Pathname',1,0,'text');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_size','File Size',1,0,'int64');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_mode','File Mode',1,0,'int64');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_xattr_name','Xattr Name',1,0,'text');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_xattr_value','Xattr Value',1,0,'text');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_mtime','File Mtime',1,0,'int64');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_ctime','File Ctime',1,0,'int64');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_size','File Size',1,1,'int64');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_mode','File Mode',1,1,'int64');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_xattr_name','Xattr Name',1,1,'text');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_xattr_value','Xattr Value',1,1,'text');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_mtime','File Mtime',1,1,'int64');
insert into appformd values (NULL,'gufibigentriesremotetraversal',5,'ENT_ctime','File Ctime',1,1,'int64');
insert into appformdtree values (NULL,'gufibigentriesremotetraversal',1,'REMOTESEARCHSUBPATH1','');
insert into appformdtree values (NULL,'gufibigentriesremotetraversal',1,'REMOTESEARCHSUBPATH2','');
