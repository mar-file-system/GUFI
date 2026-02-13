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



delete from apps where appname='vrsummaryallgroupby';
delete from appsqueries where appsqname='vrsummaryallgroupby';
delete from apppart where apppartname='vrsummaryallgroupby';
delete from appforminfo where appfname='vrsummaryallgroupby';
delete from appformd where appdname='vrsummaryallgroupby';
delete from appformdtree where appdtname='vrsummaryallgroupby';

-- you may want to change 'http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
insert into apps values (NULL,'vrsummaryallgroupby','directory summaries group by (allvar) (local) with Traversal Control','Query of directory summary  Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'vrsummaryallgroupby',directory summaries group by (allvar) (remote) with Traversal Control','Query of Remote directory summary Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
*/

insert into appsqueries values (NULL,'vrsummaryallgroupby','V','1',1);
-- these can be added to run the gufi_query remotely both command to run like ssh/pdsh and the remote host
/* these statements are used for running remote gufi
insert into appsqueries values (NULL,'vrsummaryallgroupby','R','ssh',1);
insert into appsqueries values (NULL,'vrsummaryallgroupby','A','REMOTEHOST',1);
*/
insert into appsqueries values (NULL,'vrsummaryallgroupby','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrsummaryallgroupby','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrsummaryallgroupby','J','insert into aggregate select {interimselected} from intermediate',1);
insert into appsqueries values (NULL,'vrsummaryallgroupby','G','select {interimselected} from aggregate',1);
insert into appsqueries values (NULL,'vrsummaryallgroupby','E','insert into intermediate select {selectonly} from vrsummary {where} {whereonly} {groupbyonly} {orderbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'vrsummaryallgroupby','Z','select {displayselected} from vtmp {groupbyonly} {orderbyonly} {limitrec}',1);

insert into apppart values (NULL,'vrsummaryallgroupby',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'vrsummaryallgroupby',1,'treelist','Path Options','Optionally select search path','',1,2,0);
-- insert into apppart values (NULL,'vrsummaryallgroupby',2,'input','Directory Input Criteria','Please enter at least one','',6,2,1);
insert into apppart values (NULL,'vrsummaryallgroupby',3,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
insert into apppart values (NULL,'vrsummaryallgroupby',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'vrsummaryallgroupby',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'vrsummaryallgroupby',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'vrsummaryallgroupby',7,'limit','limit results','Optionally limit results','',1,2,0);
insert into apppart values (NULL,'vrsummaryallgroupby',8,'downloadf','download results in csv','download results csv','',1,2,0);

insert into appforminfo values (NULL,'vrsummaryallgroupby',1,'textlist','treelist','treelist','treelist','treelist','Search Local Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');

insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'text','DIR_path','rpath(sname,sroll) as path','path','path','Path','Path of Dir',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'text','DIR_name','name as name','name','name','Directory Name','Name of Directory',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_nlink','nlink as nlink','nlink','nlink','number of dlinks','number of dlinks',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_uid','uid as uid','uid','uid','Directory UID','Directory UID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_gid','gid as gid','gid','gid','Directory GID','Directory GID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_size.1','size as size','size','size','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_size.2','size as size','size','size','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','Directory Mod Time','Mod time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_mtime.2','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','Directory Mod Time','Mod time of Directory ',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_ctime.1','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_ctime.2','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_atime.1','atime as atime','atime','datetime(atime,"unixepoch") as atime','Directory Access Time','Access time of Direcctory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_atime.2','atime as atime','atime','datetime(atime,"unixepoch") as atime','Direcctory Access Time','Access time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_minctime','minctime as minctime','minctime','datetime(minctime,"unixepoch") as minctime','Min file ctime','Min file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxctime','maxctime as maxctime','maxctime','datetime(maxctime,"unixepoch") as maxctime','Max file ctime','Max file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_minmtime','minmtime as minmtime','minmtime','datetime(minmtime,"unixepoch") as minmtime','Min file mtime','Min file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxmtime','maxmtime as maxmtime','maxmtime','datetime(maxmtime,"unixepoch") as maxmtime','Max file mtime','Max file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_minatimei','minatime as minatime','minatime','datetime(minatime,"unixepoch") as minatime','Min file atime','Min file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxatime','maxatime as maxatime','maxatime','datetime(maxatime,"unixepoch") as maxatime','Max file atime','Max file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_mincrtime','mincrtime as mincrtime','mincrtime','datetime(mincrtime,"unixepoch") as mincrtime','Min file crtime','Min file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxcrtime','maxcrtime as maxcrtime','maxcrtime','datetime(maxcrtime,"unixepoch") as maxcrtime','Max file crtime','Max file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totxattr','totxattr as totxattr','totxattr','totxattr','Directory dtotxattr','Directory dtotxattr',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_mode','mode as mode','mode','modetotxt(mode) as mode','Directory Mode','Mode of Directory',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totfiles','totfiles as totfiles','totfiles','totfiles','Directory total files','Directory total files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totlinks','totlinks as totlinks','totlinks','totlinks','Directory total links','Directory total links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_minsize','minsize as minsize','minsize','minsize','Directory min filesize','Directory min filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxsize','maxsize as maxsize','maxsize','maxsize','Directory max filesize','Directory max filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totzero','totzero as totzero','totzeroe','totzero','Directory total zero files','Directory total zero files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_minuid','minuid as minuid','minuid','minuid','Directory min uid','Directory min uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxuid','maxuid as maxuid','maxuid','maxuid','Directory max uid','Directory max uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_mingid','mingid as mingid','mingid','mingid','Directory min gid','Directory min gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxgid','maxgid as maxgid','maxgid','maxgid','Directory max gid','Directory max gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totltk','totltk as totltk','totltk','totltk','Directory total files ltk','Directory total files ltk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totmtk','totmtk as totmtk','totmtk','totmtk','Directory total files mtk','Directory total files mtk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totltm','totltm as totltm','totltm','totltm','Directory total files ltm','Directory total files ltm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totmtm','totmtm as totmtm','totmtm','totmtm','Directory total files mtm','Directory total files mtm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totmtg','totmtg as totmtg','totmtg','totmtg','Directory total files mtg','Directory total files mtg',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totmtt','totmtt as totmtt','totmtt','totmtt','Directory total files mtt','Directory total files mtt',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_totsize','totsize as totsize','totsize','totsize','Directory total files size','Directory total files size',1,1,0,'min=''0''','','','','','','','','','','W');

--- sums, mins, maxs
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_inode_cnt','count(inode) as inode_cnt','inode_cnt','sum(inode_cnt) as inode_cnt  ','Count of Dirs','Count of Dirs',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_size_sum','sum(size) as size_sum','size_sum','sum(size_sum) as size_sum','Sum of dir size','Sum of DIR Size',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totsize_sum','sum(totsize) as totsize_sum','totsize_sum','sum(totsize_sum) as totsize_sum','Sum of dir totsize','Sum of DIR totsize',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totxattr_sum','sum(totxattr) as totxattr_sum','totxattr_sum','sum(totxattr_sum) as totxattr_sum','Sum of dir totxattr','Sum of DIR totxattr',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totfiles_sum','sum(totfiles) as totfiles_sum','totfiles_sum','sum(totfiles_sum) as totfiles_sum','Sum of dir totfiles','Sum of DIR totfiles',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totlinks_sum','sum(totlinks) as totlinks_sum','totlinks_sum','sum(totlinks_sum) as totlinks_sum','Sum of dir totlinks','Sum of DIR totlinks',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totzero_sum','sum(totzero) as totzero_sum','totzero_sum','sum(totzero_sum) as totzero_sum','Sum of dir totzero','Sum of DIR totzero',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_minsize_min','min(minsize) as minsize_min','minsize_min','min(minsize_min) as minsize_min','min of dir minsize','min of DIR minsize',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_maxsize_max','max(maxsize) as maxsize_max','maxsize_max','max(maxsize_max) as maxsize_max','max of dir maxsize','max of DIR maxsize',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totltk_sum','sum(totltk as totltk_sum','totltk_sum','sum(totltk_sum) as totltk_sum','Sum of dir totltk','Sum of DIR totltk',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totmtk_sum','sum(totmtk as totmtk_sum','totmtk_sum','sum(totmtk_sum) as totmtk_sum','Sum of dir totmtk','Sum of DIR totmtk',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totltm_sum','sum(totltm as totltm_sum','totltm_sum','sum(totltm_sum) as totltm_sum','Sum of dir totltm','Sum of DIR totltm',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totmtm_sum','sum(totmtm as totmtm_sum','totmtm_sum','sum(totmtm_sum) as totmtm_sum','Sum of dir totmtm','Sum of DIR totmtm',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totmtg_sum','sum(totmtg as totmtg_sum','totmtg_sum','sum(totmtg_sum) as totmtg_sum','Sum of dir totmtg','Sum of DIR totmtg',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'int64','DIR_totmtt_sum','sum(totmtt as totmtt_sum','totmtt_sum','sum(totmtt_sum) as totmtt_sum','Sum of dir totmtt','Sum of DIR totmtt',2,1,0,'','','','','','','','','','','S');

---size cat
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_size_cat','case when size<1 then ''Zero'' when size<1024 then ''1-K'' when size>1023 and size<1024*1024 then ''K-M'' when size>1024*1024-1 and size<1024*1024*1024 then ''M-G'' when size>1024*1024*1024-1 and size<1024*1024*1024*1024 then ''G-T'' else ''GT1T'' end as size_cat','size_cat','size_cat','size category','size category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_totsize_cat','case when totsize<1 then ''Zero'' when totsize<1024 then ''1-K'' when totsize>1023 and totsize<1024*1024 then ''K-M'' when totsize>1024*1024-1 and totsize<1024*1024*1024 then ''M-G'' when totsize>1024*1024*1024-1 and totsize<1024*1024*1024*1024 then ''G-T'' else ''GT1T'' end as totsize_cat','totsize_cat','totsize_cat','totsize category','totsize category',2,1,0,'','','','','','','','','','','S');

--- date cat
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_mtime_cat','case when unixepoch(''now'')-mtime<86400 then ''0D-1D'' when unixepoch(''now'')-mtime>86400-1 and unixepoch(''now'')-mtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-mtime>86400*7-1 and unixepoch(''now'')-mtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-mtime>86400*30-1 and unixepoch(''now'')-mtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-mtime>86400*365-1 and unixepoch(''now'')-mtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-mtime>86400*365*2-1  and unixepoch(''now'')-mtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-mtime>86400*365*5-1  and unixepoch(''now'')-mtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-mtime>86400*365*10-1  and unixepoch(''now'')-mtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as mtime_cat','mtime_cat','mtime_cat','mtime category','mtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_ctime_cat','case when unixepoch(''now'')-ctime<86400 then ''0D-1D'' when unixepoch(''now'')-ctime>86400-1 and unixepoch(''now'')-ctime<86400*7 then ''1D-1W'' when unixepoch(''now'')-ctime>86400*7-1 and unixepoch(''now'')-ctime<86400*30 then ''1W-1M'' when unixepoch(''now'')-ctime>86400*30-1 and unixepoch(''now'')-ctime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-ctime>86400*365-1 and unixepoch(''now'')-ctime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-ctime>86400*365*2-1  and unixepoch(''now'')-ctime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-ctime>86400*365*5-1  and unixepoch(''now'')-ctime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-ctime>86400*365*10-1  and unixepoch(''now'')-ctime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as ctime_cat','ctime_cat','ctime_cat','ctime category','ctime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_atime_cat','case when unixepoch(''now'')-atime<86400 then ''0D-1D'' when unixepoch(''now'')-atime>86400-1 and unixepoch(''now'')-atime<86400*7 then ''1D-1W'' when unixepoch(''now'')-atime>86400*7-1 and unixepoch(''now'')-atime<86400*30 then ''1W-1M'' when unixepoch(''now'')-atime>86400*30-1 and unixepoch(''now'')-atime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-atime>86400*365-1 and unixepoch(''now'')-atime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-atime>86400*365*2-1  and unixepoch(''now'')-atime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-atime>86400*365*5-1  and unixepoch(''now'')-atime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-atime>86400*365*10-1  and unixepoch(''now'')-atime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as atime_cat','atime_cat','atime_cat','atime category','atime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_crtime_cat','case when unixepoch(''now'')-crtime<86400 then ''0D-1D'' when unixepoch(''now'')-crtime>86400-1 and unixepoch(''now'')-crtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-crtime>86400*7-1 and unixepoch(''now'')-crtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-crtime>86400*30-1 and unixepoch(''now'')-crtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-crtime>86400*365-1 and unixepoch(''now'')-crtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-crtime>86400*365*2-1  and unixepoch(''now'')-crtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-crtime>86400*365*5-1  and unixepoch(''now'')-crtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-crtime>86400*365*10-1  and unixepoch(''now'')-crtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as crtime_cat','crtime_cat','crtime_cat','crtime category','mtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_minmtime_cat','case when unixepoch(''now'')-minmtime<86400 then ''0D-1D'' when unixepoch(''now'')-minmtime>86400-1 and unixepoch(''now'')-minmtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-minmtime>86400*7-1 and unixepoch(''now'')-minmtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-minmtime>86400*30-1 and unixepoch(''now'')-minmtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-minmtime>86400*365-1 and unixepoch(''now'')-minmtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-minmtime>86400*365*2-1  and unixepoch(''now'')-minmtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-minmtime>86400*365*5-1  and unixepoch(''now'')-minmtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-minmtime>86400*365*10-1  and unixepoch(''now'')-minmtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as minmtime_cat','minmtime_cat','minmtime_cat','minmtime category','minmtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_maxmtime_cat','case when unixepoch(''now'')-maxmtime<86400 then ''0D-1D'' when unixepoch(''now'')-maxmtime>86400-1 and unixepoch(''now'')-maxmtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-maxmtime>86400*7-1 and unixepoch(''now'')-maxmtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-maxmtime>86400*30-1 and unixepoch(''now'')-maxmtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-maxmtime>86400*365-1 and unixepoch(''now'')-maxmtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-maxmtime>86400*365*2-1  and unixepoch(''now'')-maxmtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-maxmtime>86400*365*5-1  and unixepoch(''now'')-maxmtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-maxmtime>86400*365*10-1  and unixepoch(''now'')-maxmtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as maxmtime_cat','maxmtime_cat','maxmtime_cat','maxmtime category','maxmtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_minctime_cat','case when unixepoch(''now'')-minctime<86400 then ''0D-1D'' when unixepoch(''now'')-minctime>86400-1 and unixepoch(''now'')-minctime<86400*7 then ''1D-1W'' when unixepoch(''now'')-minctime>86400*7-1 and unixepoch(''now'')-minctime<86400*30 then ''1W-1M'' when unixepoch(''now'')-minctime>86400*30-1 and unixepoch(''now'')-minctime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-minctime>86400*365-1 and unixepoch(''now'')-minctime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-minctime>86400*365*2-1  and unixepoch(''now'')-minctime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-minctime>86400*365*5-1  and unixepoch(''now'')-minctime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-minctime>86400*365*10-1  and unixepoch(''now'')-minctime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as minctime_cat','minctime_cat','minctime_cat','minctime category','minctime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_maxctime_cat','case when unixepoch(''now'')-maxctime<86400 then ''0D-1D'' when unixepoch(''now'')-maxctime>86400-1 and unixepoch(''now'')-maxctime<86400*7 then ''1D-1W'' when unixepoch(''now'')-maxctime>86400*7-1 and unixepoch(''now'')-maxctime<86400*30 then ''1W-1M'' when unixepoch(''now'')-maxctime>86400*30-1 and unixepoch(''now'')-maxctime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-maxctime>86400*365-1 and unixepoch(''now'')-maxctime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-maxctime>86400*365*2-1  and unixepoch(''now'')-maxctime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-maxctime>86400*365*5-1  and unixepoch(''now'')-maxctime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-maxctime>86400*365*10-1  and unixepoch(''now'')-maxctime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as maxctime_cat','maxctime_cat','maxctime_cat','maxctime category','maxctime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_minatime_cat','case when unixepoch(''now'')-minatime<86400 then ''0D-1D'' when unixepoch(''now'')-minatime>86400-1 and unixepoch(''now'')-minatime<86400*7 then ''1D-1W'' when unixepoch(''now'')-minatime>86400*7-1 and unixepoch(''now'')-minatime<86400*30 then ''1W-1M'' when unixepoch(''now'')-minatime>86400*30-1 and unixepoch(''now'')-minatime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-minatime>86400*365-1 and unixepoch(''now'')-minatime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-minatime>86400*365*2-1  and unixepoch(''now'')-minatime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-minatime>86400*365*5-1  and unixepoch(''now'')-minatime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-minatime>86400*365*10-1  and unixepoch(''now'')-minatime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as minatime_cat','minatime_cat','minatime_cat','minatime category','minatime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_maxatime_cat','case when unixepoch(''now'')-maxatime<86400 then ''0D-1D'' when unixepoch(''now'')-maxatime>86400-1 and unixepoch(''now'')-maxatime<86400*7 then ''1D-1W'' when unixepoch(''now'')-maxatime>86400*7-1 and unixepoch(''now'')-maxatime<86400*30 then ''1W-1M'' when unixepoch(''now'')-maxatime>86400*30-1 and unixepoch(''now'')-maxatime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-maxatime>86400*365-1 and unixepoch(''now'')-maxatime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-maxatime>86400*365*2-1  and unixepoch(''now'')-maxatime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-maxatime>86400*365*5-1  and unixepoch(''now'')-maxatime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-maxatime>86400*365*10-1  and unixepoch(''now'')-maxatime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as maxatime_cat','maxatime_cat','maxatime_cat','maxatime category','maxatime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_mincrtime_cat','case when unixepoch(''now'')-mincrtime<86400 then ''0D-1D'' when unixepoch(''now'')-mincrtime>86400-1 and unixepoch(''now'')-mincrtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-mincrtime>86400*7-1 and unixepoch(''now'')-mincrtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-mincrtime>86400*30-1 and unixepoch(''now'')-mincrtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-mincrtime>86400*365-1 and unixepoch(''now'')-mincrtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-mincrtime>86400*365*2-1  and unixepoch(''now'')-mincrtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-mincrtime>86400*365*5-1  and unixepoch(''now'')-mincrtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-mincrtime>86400*365*10-1  and unixepoch(''now'')-mincrtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as mincrtime_cat','mincrtime_cat','mincrtime_cat','mincrtime category','mincrtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryallgroupby',4,'text','DIR_maxcrtime_cat','case when unixepoch(''now'')-maxcrtime<86400 then ''0D-1D'' when unixepoch(''now'')-maxcrtime>86400-1 and unixepoch(''now'')-maxcrtime<86400*7 then ''1D-1W'' when unixepoch(''now'')-maxcrtime>86400*7-1 and unixepoch(''now'')-maxcrtime<86400*30 then ''1W-1M'' when unixepoch(''now'')-maxcrtime>86400*30-1 and unixepoch(''now'')-maxcrtime<86400*365 then ''1M-1Y'' when unixepoch(''now'')-maxcrtime>86400*365-1 and unixepoch(''now'')-maxcrtime<86400*365*2 then ''1-2Y'' when unixepoch(''now'')-maxcrtime>86400*365*2-1  and unixepoch(''now'')-maxcrtime<86400*365*5 then ''2-5Y'' when unixepoch(''now'')-maxcrtime>86400*365*5-1  and unixepoch(''now'')-maxcrtime<86400*365*10 then ''5-10y'' when unixepoch(''now'')-maxcrtime>86400*365*10-1  and unixepoch(''now'')-maxcrtime<86400*365*20 then ''10-20Y'' else ''GT20Y'' end as maxcrtime_cat','maxcrtime_cat','maxcrtime_cat','maxcrtime category','maxcrtime category',2,1,0,'','','','','','','','','','','S');

insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_mincrtime','mincrtime as mincrtime','mincrtime','datetime(mincrtime,"unixepoch") as mincrtime','Min file crtime','Min file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryallgroupby',3,'int64','DIR_maxcrtime','maxcrtime as maxcrtime','maxcrtime','datetime(maxcrtime,"unixepoch") as maxcrtime','Max file crtime','Max file crtime',1,1,1,'','','','','','','','','','','W');

insert into appforminfo values (NULL,'vrsummaryallgroupby',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');

insert into appforminfo values (NULL,'vrsummaryallgroupby',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');

insert into appforminfo values (NULL,'vrsummaryallgroupby',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');

insert into appforminfo values (NULL,'vrsummaryallgroupby',8,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');

insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_inode_cnt','Dir inode cnt',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_size_sum','Dir sizesum',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totsize_sum','Dir totsizesum',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totxattr_sum','Dir totxattr',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totfiles_sum','Dir totfiles',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totlinks_sum','Dir totlinks',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totzero_sum','Dir totzero',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_minsize_min','Dir minsize',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_maxsize_max','Dir maxsize',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totltk_sum','Dir totltk',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totmtk_sum','Dir totmtk',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totltm_sum','Dir totltm',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totmtm_sum','Dir totmtm',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totmtg_sum','Dir totmtg',1,0,'int64');
insert into appformd values (NULL,'vrsummaryallgroupby',5,'DIR_totmtt_sum','Dir totmtt',1,0,'int64');

---- need appformd for groupby
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_size_cat','size category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_totsize_cat','totsize category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_mtime_cat','mtime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_ctime_cat','ctime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_atime_cat','atime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_crtime_cat','crtime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_minmtime_cat','minmtime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_maxmtime_cat','maxmtime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_minctime_cat','minctime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_maxctime_cat','maxctime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_minatime_cat','minatime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_maxatime_cat','maxatime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_mincrtime_cat','mincrtime category',1,0,'text');
insert into appformd values (NULL,'vrsummaryallgroupby',6,'DIR_maxcrtime_cat','maxcrtime category',1,0,'text');


-- you may want to put in your own path list for LOCALSEARCHSUBPATHn  you can have as many as you like
insert into appformdtree values (NULL,'vrsummaryallgroupby',1,'LOCALSEARCHSUBPATH1','');
insert into appformdtree values (NULL,'vrsummaryallgroupby',1,'LOCALSEARCHSUBPATH2','');
