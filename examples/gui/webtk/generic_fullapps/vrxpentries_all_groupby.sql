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



delete from apps where appname='vrxpentriesallgroupby';
delete from appsqueries where appsqname='vrxpentriesallgroupby';
delete from apppart where apppartname='vrxpentriesallgroupby';
delete from appforminfo where appfname='vrxpentriesallgroupby';
delete from appformd where appdname='vrxpentriesallgroupby';
delete from appformdtree where appdtname='vrxpentriesallgroupby';

-- you may want to change 'http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
insert into apps values (NULL,'vrxpentriesallgroupby','files groupby (allvar) (local) with Traversal Control','Query of local File Entries groupby Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'vrxpentriesallgroupby',files groupby (allvar) (remote) with Traversal Control','Query of Remote File Entries groupby Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
*/

insert into appsqueries values (NULL,'vrxpentriesallgroupby','V','1',1);
-- these can be added to run the gufi_query remotely both command to run like ssh/pdsh and the remote host
/* these statements are used for running remote gufi
insert into appsqueries values (NULL,'vrxpentriesallgroupby','R','ssh',1);
insert into appsqueries values (NULL,'vrxpentriesallgroupby','A','REMOTEHOST',1);
*/
insert into appsqueries values (NULL,'vrxpentriesallgroupby','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrxpentriesallgroupby','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrxpentriesallgroupby','J','insert into aggregate select {interimselected} from intermediate',1);
insert into appsqueries values (NULL,'vrxpentriesallgroupby','G','select {interimselected} from aggregate' ,1);
insert into appsqueries values (NULL,'vrxpentriesallgroupby','E','insert into intermediate select {selectonly} from vrxpentries {where} {whereonly} {groupbyonly} {orderbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'vrxpentriesallgroupby','Z','select {displayselected} from vtmp {groupbyonly} {orderbyonly} {limitrec}',1);


insert into apppart values (NULL,'vrxpentriesallgroupby',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'vrxpentriesallgroupby',1,'treelist','Path Options','Optionally select search path','',1,2,0);
insert into apppart values (NULL,'vrxpentriesallgroupby',2,'input','File Input Criteria','Please enter at least one','',6,2,1);
insert into apppart values (NULL,'vrxpentriesallgroupby',3,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
insert into apppart values (NULL,'vrxpentriesallgroupby',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'vrxpentriesallgroupby',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'vrxpentriesallgroupby',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'vrxpentriesallgroupby',7,'limit','limit results','Optionally limit results','',1,2,0);
insert into apppart values (NULL,'vrxpentriesallgroupby',8,'downloadf','download results in csv','download results csv','',1,2,0);

insert into appforminfo values (NULL,'vrxpentriesallgroupby',1,'textlist','treelist','treelist','treelist','treelist','Search Local Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');

insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','ENT_path','rpath(sname,sroll) as path','path','path','Path','Path of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','ENT_name','name as name','name','name','File Name','Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','ENT_type','type as type','type','type','Entry Type','Entry type',1,1,0,'pattern=''[dfl]+''','placeholder=''d or f or l''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_nlink','nlink as nlink','nlink','nlink','number of links','number of links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','ENT_linkname','linkname as linkname','linkname','linkname','Link Name','Link Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_size.1','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_size.2','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'pattern=''[A-Za-z0-9]+''','placeholder=''%str%''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_uid','uid as uid','uid','uid','File UID','File UID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_gid','gid as gid','gid','gid','File GID','File GID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_mtime.2','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_ctime.1','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_ctime.2','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_atime.1','atime as atime','atime','datetime(atime,"unixepoch") as atime','File Access Time','Access time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_atime.2','atime as atime','atime','datetime(atime,"unixepoch") as atime','File Access Time','Access time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_crtime.1','crtime as crtime','crtime','datetime(crtime,"unixepoch") as crtime','File Create Time','Create time of File',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'int64','ENT_crtime.2','crtime as crtime','crtime','datetime(crtime,"unixepoch") as crtime','File Create Time','Create time of File',1,1,1,'','','','','','','','','','','W');

insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'text','DIR_name','dname as dname','dname','dname','Directory Name','Name of Directory',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dnlink','dnlink as dnlink','dnlink','dnlink','number of dlinks','number of dlinks',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_duid','duid as duid','duid','duid','Directory UID','Directory UID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dgid','dgid as dgid','dgid','dgid','Directory GID','Directory GID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dsize.1','dsize as dsize','dsize','dsize','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dsize.2','dsize as dsize','dsize','dsize','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmtime.1','dmtime as dmtime','dmtime','datetime(dmtime,"unixepoch") as dmtime','Directory Mod Time','Mod time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmtime.2','dmtime as dmtime','dmtime','datetime(dmtime,"unixepoch") as dmtime','Directory Mod Time','Mod time of Directory ',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dctime.1','dctime as dctime','dctime','datetime(dctime,"unixepoch") as dctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dctime.2','dctime as dctime','dctime','datetime(dctime,"unixepoch") as dctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_datime.1','datime as datime','datime','datetime(datime,"unixepoch") as datime','Directory Access Time','Access time of Direcctory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_datime.2','datime as datime','datime','datetime(datime,"unixepoch") as datime','Direcctory Access Time','Access time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dminctime','dminctime as dminctime','dminctime','datetime(dminctime,"unixepoch") as dminctime','Min file ctime','Min file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmaxctime','dmaxctime as dmaxctime','dmaxctime','datetime(dmaxctime,"unixepoch") as dmaxctime','Max file ctime','Max file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dminmtime','dminmtime as dminmtime','dminmtime','datetime(dminmtime,"unixepoch") as dminmtime','Min file mtime','Min file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmaxmtime','dmaxmtime as dmaxmtime','dmaxmtime','datetime(dmaxmtime,"unixepoch") as dmaxmtime','Max file mtime','Max file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dminatimei','dminatime as dminatime','dminatime','datetime(dminatime,"unixepoch") as dminatime','Min file atime','Min file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmaxatime','dmaxatime as dmaxatime','dmaxatime','datetime(dmaxatime,"unixepoch") as dmaxatime','Max file atime','Max file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmincrtime','dmincrtime as dmincrtime','dmincrtime','datetime(dmincrtime,"unixepoch") as dmincrtime','Min file crtime','Min file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmaxcrtime','dmaxcrtime as dmaxcrtime','dmaxcrtime','datetime(dmaxcrtime,"unixepoch") as dmaxcrtime','Max file crtime','Max file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',2,'text','DIR_dlinkname','dlinkname as dlinkname','dlinkname','dlinkname','DLink Name','DLink Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_ddepth','ddepth as ddepth','ddepth','ddepth','Directory depth','Directory depth',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotxattr','dtotxattr as dtotxattr','dtotxattr','dtotxattr','Directory dtotxattr','Directory dtotxattr',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmode','dmode as dmode','dmode','modetotxt(dmode) as dmode','Directory Mode','Mode of Directory',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotfiles','dtotfiles as dtotfiles','dtotfiles','dtotfiles','Directory total files','Directory total files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotlinks','dtotlinks as dtotlinks','dtotlinks','dtotlinks','Directory total links','Directory total links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dminsize','dminsize as dminsize','dminsize','dminsize','Directory min filesize','Directory min filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmaxsize','dmaxsize as dmaxsize','dmaxsize','dmaxsize','Directory max filesize','Directory max filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotzero','dtotzero as dtotzero','dtotzeroe','dtotzero','Directory total zero files','Directory total zero files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dminuid','dminuid as dminuid','dminuid','dminuid','Directory min uid','Directory min uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmaxuid','dmaxuid as dmaxuid','dmaxuid','dmaxuid','Directory max uid','Directory max uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmingid','dmingid as dmingid','dmingid','dmingid','Directory min gid','Directory min gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dmaxgid','dmaxgid as dmaxgid','dmaxgid','dmaxgid','Directory max gid','Directory max gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotltk','dtotltk as dtotltk','dtotltk','dtotltk','Directory total files ltk','Directory total files ltk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotmtk','dtotmtk as dtotmtk','dtotmtk','dtotmtk','Directory total files mtk','Directory total files mtk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotltm','dtotltm as dtotltm','dtotltm','dtotltm','Directory total files ltm','Directory total files ltm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotmtm','dtotmtm as dtotmtm','dtotmtm','dtotmtm','Directory total files mtm','Directory total files mtm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotmtg','dtotmtg as dtotmtg','dtotmtg','dtotmtg','Directory total files mtg','Directory total files mtg',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotmtt','dtotmtt as dtotmtt','dtotmtt','dtotmtt','Directory total files mtt','Directory total files mtt',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',3,'int64','DIR_dtotsize','dtotsize as dtotsize','dtotsize','dtotsize','Directory total files size','Directory total files size',1,1,0,'min=''0''','','','','','','','','','','W');

--- since this is groupby for vrxpentries (essentiall its the entries part only --
--- the -ikj queries will be more like a group by where we sum the counts as we traverse --
--- there will be a groupby for summary that will cover all the summary parts --

insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'int64','ENT_inode_cnt','count(inode) as inode_cnt','inode_cnt','sum(inode_cnt) as inode_cnt  ','Count of files','Count of Files',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'int64','ENT_size_sum','sum(size) as size_sum','size_sum','sum(size_sum) as size_sum','Sum of file size','Sum of File Size',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'text','ENT_ext_cat','case when instr(substr(name,-5,5),''.'')>0 then substr(substr(name,-5,5),instr(substr(name,-5,5),''.'')+1) else '''' end as ext_cat','ext_cat','ext_cat','file extention category','file extension category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'text','ENT_size_cat','case when size<1 then ''Zero'' when size<1024 then ''1-K'' when size>1023 and size<1024*1024 then ''K-M'' when size>1024*1024-1 and size<1024*1024*1024 then ''M-G'' when size>1024*1024*1024-1 and size<1024*1024*1024*1024 then ''G-T'' else ''GT1T'' end as size_cat','size_cat','size_cat','size category','size category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'text','ENT_mtime_cat','case when unixepoch(''now'')-mtime<86400 then ''0D-1D'' when unixepoch(''now'')-mtime>86400-1 and unixepoch(''now'')-mtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-mtime>86400*7-1 and unixepoch(''now'')-mtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-mtime>86400*30-1 and unixepoch(''now'')-mtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-mtime>86400*365-1 and unixepoch(''now'')-mtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-mtime>86400*365*2-1  and unixepoch(''now'')-mtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-mtime>86400*365*5-1  and unixepoch(''now'')-mtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-mtime>86400*365*10-1  and unixepoch(''now'')-mtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as mtime_cat','mtime_cat','mtime_cat','mtime category','mtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'text','ENT_ctime_cat','case when unixepoch(''now'')-ctime<86400 then ''0D-1D'' when unixepoch(''now'')-ctime>86400-1 and unixepoch(''now'')-ctime<86400*7 then ''1D-1W'' when unixepoch(''now'')-ctime>86400*7-1 and unixepoch(''now'')-ctime<86400*30 then ''1W-1M'' when unixepoch(''now'')-ctime>86400*30-1 and unixepoch(''now'')-ctime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-ctime>86400*365-1 and unixepoch(''now'')-ctime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-ctime>86400*365*2-1  and unixepoch(''now'')-ctime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-ctime>86400*365*5-1  and unixepoch(''now'')-ctime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-ctime>86400*365*10-1  and unixepoch(''now'')-ctime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as ctime_cat','ctime_cat','ctime_cat','ctime category','ctime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'text','ENT_atime_cat','case when unixepoch(''now'')-atime<86400 then ''0D-1D'' when unixepoch(''now'')-atime>86400-1 and unixepoch(''now'')-atime<86400*7 then ''1D-1W'' when unixepoch(''now'')-atime>86400*7-1 and unixepoch(''now'')-atime<86400*30 then ''1W-1M'' when unixepoch(''now'')-atime>86400*30-1 and unixepoch(''now'')-atime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-atime>86400*365-1 and unixepoch(''now'')-atime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-atime>86400*365*2-1  and unixepoch(''now'')-atime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-atime>86400*365*5-1  and unixepoch(''now'')-atime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-atime>86400*365*10-1  and unixepoch(''now'')-atime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as atime_cat','atime_cat','atime_cat','mtime category','mtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriesallgroupby',4,'text','ENT_crtime_cat','case when unixepoch(''now'')-crtime<86400 then ''0D-1D'' when unixepoch(''now'')-crtime>86400-1 and unixepoch(''now'')-crtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-crtime>86400*7-1 and unixepoch(''now'')-crtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-crtime>86400*30-1 and unixepoch(''now'')-crtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-crtime>86400*365-1 and unixepoch(''now'')-crtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-crtime>86400*365*2-1  and unixepoch(''now'')-crtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-crtime>86400*365*5-1  and unixepoch(''now'')-crtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-crtime>86400*365*10-1  and unixepoch(''now'')-crtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as crtime_cat','crtime_cat','crtime_cat','crtime category','crtime category',2,1,0,'','','','','','','','','','','S');


insert into appforminfo values (NULL,'vrxpentriesallgroupby',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');

insert into appforminfo values (NULL,'vrxpentriesallgroupby',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');

insert into appforminfo values (NULL,'vrxpentriesallgroupby',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');

insert into appforminfo values (NULL,'vrxpentriesallgroupby',8,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');


--- appformd values need to be same as select values
insert into appformd values (NULL,'vrxpentriesallgroupby',5,'ENT_size_sum','File Size',1,0,'int64');
insert into appformd values (NULL,'vrxpentriesallgroupby',5,'ENT_inode_cnt','inode cnt',1,0,'int64');

---- need appformd for groupby
insert into appformd values (NULL,'vrxpentriesallgroupby',6,'ENT_type','type category',1,0,'text');
insert into appformd values (NULL,'vrxpentriesallgroupby',6,'ENT_size_cat','size category',1,0,'text');
insert into appformd values (NULL,'vrxpentriesallgroupby',6,'ENT_mtime_cat','mtime category',1,0,'text');
insert into appformd values (NULL,'vrxpentriesallgroupby',6,'ENT_ctime_cat','ctime category',1,0,'text');
insert into appformd values (NULL,'vrxpentriesallgroupby',6,'ENT_atime_cat','atime category',1,0,'text');
insert into appformd values (NULL,'vrxpentriesallgroupby',6,'ENT_crtime_cat','crtime category',1,0,'text');
insert into appformd values (NULL,'vrxpentriesallgroupby',6,'ENT_ext_cat','ext category',1,0,'text');

-- you may want to put in your own path list for LOCALSEARCHSUBPATHn  you can have as many as you like
insert into appformdtree values (NULL,'vrxpentriesallgroupby',1,'LOCALSEARCHSUBPATH1','');
insert into appformdtree values (NULL,'vrxpentriesallgroupby',1,'LOCALSEARCHSUBPATH2','');
