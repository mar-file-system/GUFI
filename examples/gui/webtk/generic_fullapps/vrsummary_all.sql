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



delete from apps where appname='vrsummaryall';
delete from appsqueries where appsqname='vrsummaryall';
delete from apppart where apppartname='vrsummaryall';
delete from appforminfo where appfname='vrsummaryall';
delete from appformd where appdname='vrsummaryall';
delete from appformdtree where appdtname='vrsummaryall';

-- you may want to change 'http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
insert into apps values (NULL,'vrsummaryall','directory summaries (allvar) (local) with Traversal Control','Query of directory summary  Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'vrsummaryall',directory summaries (allvar) (remote) with Traversal Control','Query of Remote directory summary Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
*/

insert into appsqueries values (NULL,'vrsummaryall','V','1',1);
-- these can be added to run the gufi_query remotely both command to run like ssh/pdsh and the remote host
/* these statements are used for running remote gufi
insert into appsqueries values (NULL,'vrsummaryall','R','ssh',1);
insert into appsqueries values (NULL,'vrsummaryall','A','REMOTEHOST',1);
*/
insert into appsqueries values (NULL,'vrsummaryall','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrsummaryall','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrsummaryall','J','insert into aggregate select {interimselected} from intermediate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'vrsummaryall','G','select {interimselected} from aggregate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'vrsummaryall','E','insert into intermediate select {selectonly} from vrsummary {where} {whereonly} {orderbyselected} {groupbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'vrsummaryall','Z','select {displayselected} from vtmp {orderbyselected} {groupbyonly} {limitrec}',1);

insert into apppart values (NULL,'vrsummaryall',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'vrsummaryall',1,'treelist','Path Options','Optionally select search path','',1,2,0);
-- insert into apppart values (NULL,'vrsummaryall',2,'input','Directory Input Criteria','Please enter at least one','',6,2,1);
insert into apppart values (NULL,'vrsummaryall',3,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
insert into apppart values (NULL,'vrsummaryall',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'vrsummaryall',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'vrsummaryall',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'vrsummaryall',7,'limit','limit results','Optionally limit results','',1,2,0);
insert into apppart values (NULL,'vrsummaryall',8,'downloadf','download results in csv','download results csv','',1,2,0);

insert into appforminfo values (NULL,'vrsummaryall',1,'textlist','treelist','treelist','treelist','treelist','Search Local Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');

insert into appforminfo values (NULL,'vrsummaryall',3,'text','DIR_path','rpath(sname,sroll) as path','path','path','Path','Path of Dir',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'text','DIR_name','name as name','name','name','Directory Name','Name of Directory',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_nlink','nlink as nlink','nlink','nlink','number of dlinks','number of dlinks',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_uid','uid as uid','uid','uid','Directory UID','Directory UID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_gid','gid as gid','gid','gid','Directory GID','Directory GID',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_size.1','size as size','size','size','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_size.2','size as size','size','size','Directory Size','Size of Directory',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','Directory Mod Time','Mod time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_mtime.2','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','Directory Mod Time','Mod time of Directory ',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_ctime.1','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_ctime.2','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','Directory Change Time','Change time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_atime.1','atime as atime','atime','datetime(atime,"unixepoch") as atime','Directory Access Time','Access time of Direcctory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_atime.2','atime as atime','atime','datetime(atime,"unixepoch") as atime','Direcctory Access Time','Access time of Directory',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_minctime','minctime as minctime','minctime','datetime(minctime,"unixepoch") as minctime','Min file ctime','Min file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_maxctime','maxctime as maxctime','maxctime','datetime(maxctime,"unixepoch") as maxctime','Max file ctime','Max file ctime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_minmtime','minmtime as minmtime','minmtime','datetime(minmtime,"unixepoch") as minmtime','Min file mtime','Min file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_maxmtime','maxmtime as maxmtime','maxmtime','datetime(maxmtime,"unixepoch") as maxmtime','Max file mtime','Max file mtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_minatimei','minatime as minatime','minatime','datetime(minatime,"unixepoch") as minatime','Min file atime','Min file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_maxatime','maxatime as maxatime','maxatime','datetime(maxatime,"unixepoch") as maxatime','Max file atime','Max file atime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_mincrtime','mincrtime as mincrtime','mincrtime','datetime(mincrtime,"unixepoch") as mincrtime','Min file crtime','Min file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_maxcrtime','maxcrtime as maxcrtime','maxcrtime','datetime(maxcrtime,"unixepoch") as maxcrtime','Max file crtime','Max file crtime',1,1,1,'','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totxattr','totxattr as totxattr','totxattr','totxattr','Directory dtotxattr','Directory dtotxattr',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_mode','mode as mode','mode','modetotxt(mode) as mode','Directory Mode','Mode of Directory',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totfiles','totfiles as totfiles','totfiles','totfiles','Directory total files','Directory total files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totlinks','totlinks as totlinks','totlinks','totlinks','Directory total links','Directory total links',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_minsize','minsize as minsize','minsize','minsize','Directory min filesize','Directory min filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_maxsize','maxsize as maxsize','maxsize','maxsize','Directory max filesize','Directory max filesize',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totzero','totzero as totzero','totzeroe','totzero','Directory total zero files','Directory total zero files',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_minuid','minuid as minuid','minuid','minuid','Directory min uid','Directory min uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_maxuid','maxuid as maxuid','maxuid','maxuid','Directory max uid','Directory max uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_mingid','mingid as mingid','mingid','mingid','Directory min gid','Directory min gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_maxgid','maxgid as maxgid','maxgid','maxgid','Directory max gid','Directory max gid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totltk','totltk as totltk','totltk','totltk','Directory total files ltk','Directory total files ltk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totmtk','totmtk as totmtk','totmtk','totmtk','Directory total files mtk','Directory total files mtk',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totltm','totltm as totltm','totltm','totltm','Directory total files ltm','Directory total files ltm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totmtm','totmtm as totmtm','totmtm','totmtm','Directory total files mtm','Directory total files mtm',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totmtg','totmtg as totmtg','totmtg','totmtg','Directory total files mtg','Directory total files mtg',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totmtt','totmtt as totmtt','totmtt','totmtt','Directory total files mtt','Directory total files mtt',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrsummaryall',3,'int64','DIR_totsize','totsize as totsize','totsize','totsize','Directory total files size','Directory total files size',1,1,0,'min=''0''','','','','','','','','','','W');

insert into appforminfo values (NULL,'vrsummaryall',4,'text','DIR_pathname','rpath(sname,sroll)||''/''||name as pathname','pathname','pathname','Pathname','Name of Dir',1,1,0,'required','checked','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_mode','mode as mode','mode','modetotxt(mode) as mode','Dir DMode','DMode of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_nlink','nlink as nlink','nlink','nlink','Dir nlink','nlink of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_uid','uid as duid','uid','uid','Dir uid','uid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_gid','gid as dgid','gid','gid','Dir gid','gid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_size','size as size','size','size','Dir size','size of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_mtime','mtime as mtime','mtime','datetime(mtime,"unixepoch") as dmtime','Dir Mod Time','Mod time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_ctime','ctime as ctime','ctime','datetime(ctime,"unixepoch") as dctime','Dir Change Time','Change time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_atime','atime as atime','atime','datetime(atime,"unixepoch") as datime','Dir Access Time','Access time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'text','DIR_linkname','linkname as linkname','linkname','linkname','dlinkname','dlinkname',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totfiles','totfiles as totfiles','totfiles','totfiles','Dir totfiles ','totfiles of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totlinks','totlinks as totlinks','totlinks','totlinks','Dir totlinks ','totlinks of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_minuid','minuid as minuid','minuid','minuid','Dir minuid','minuid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_maxuid','maxuid as maxuid','maxuid','maxuid','Dir maxuid','maxuid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_mingid','mingid as mingid','mingid','mingid','Dir mingid','mingid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_maxgid','maxgid as maxgid','maxgid','maxgid','Dir maxgid','maxgid of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_minsize','minsize as minsize','minsize','minsize','Dir minsize','minsize of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_maxsize','maxsize as maxsize','maxsize','maxsize','Dir maxsize','maxsize of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totzero','totzero as totzero','totzero','totzero','Dir totzero','totzero of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totltk','totltk as totltk','totltk','totltk','Dir totltk','totltk of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totmtk','totmtk as totmtk','totmtk','totmtk','Dir totmtk','totmtk of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totltm','totltm as totltm','totltm','totltm','Dir totltm','totltm of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totmtm','totmtm as totmtm','totmtm','totmtm','Dir totmtm','totmtm of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totmtg','totmtg as totmtg','totmtg','totmtg','Dir totmtg','totmtg of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totmtt','totmtt as totmtt','totmtt','totmtg','Dir totmtt','totmtt of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totsize','totsize as totsize','totsize','totsize','Dir totsize','totsize of Dir',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_minctime','minctime as minctime','minctime','datetime(minctime,"unixepoch") as dminctime','Dir minctime','minctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_maxctime','maxctime as maxctime','maxctime','datetime(maxctime,"unixepoch") as dmaxctime','Dir maxctime','maxctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_minmtime','minmtime as minmtime','minmtime','datetime(minmtime,"unixepoch") as dminmtime','Dir minmtime','minctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_maxmtime','maxmtime as maxmtime','maxmtime','datetime(maxmtime,"unixepoch") as dmaxmtime','Dir maxmtime','maxctime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_minatime','minatime as minatime','minatime','datetime(minatime,"unixepoch") as dminatime','Dir minatime','minatime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_maxatime','maxatime as maxatime','maxatime','datetime(maxatime,"unixepoch") as dmaxatime','Dir maxatime','maxatime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_mincrtime','mincrtime as mincrtime','mincrtime','datetime(mincrtime,"unixepoch") as dmincrtime','Dir mincrtime','mincrtime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_maxcrtime','maxcrtime as maxcrtime','maxcrtime','datetime(maxcrtime,"unixepoch") as dmaxcrtime','Dir maxcrtime','maxcrtime time of DIR',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrsummaryall',4,'int64','DIR_totxattr','totxattr as totxattr','totxattr','totxattr','Dir totxattr','totxattr of Dir',1,1,0,'','','','','','','','','','','S');

insert into appforminfo values (NULL,'vrsummaryall',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');

insert into appforminfo values (NULL,'vrsummaryall',5,'text','orderby2','orderby2','orderby2','orderby2','Second Order By','Second Order By',1,1,0,'','','','','','','','','','','O');
insert into appforminfo values (NULL,'vrsummaryall',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');

insert into appforminfo values (NULL,'vrsummaryall',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');

insert into appforminfo values (NULL,'vrsummaryall',8,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');

insert into appformd values (NULL,'vrsummaryall',5,'DIR_pathname','Pathname',1,0,'text');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mode','Dir DMode',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_nlink','Dir Dnlink',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_uid','Dir Duid',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_gid','Dir Dgid',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_size','Dir Dsize',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mtime','Dir Dmtime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_ctime','Dir Dctime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_atime','Dir Datime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_linkname','Dir Dlinkname',1,0,'text');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totfiles','Dir Dtotfiles',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totlinks','Dir Dtotlinks',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minuid','Dir Dminuid',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxuid','Dir Dmaxuid',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mingid','Dir Dmingid',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxgid','Dir Dmaxgid',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minsize','Dir Dminsize',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxsize','Dir Dmaxsize',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totzero','Dir Dtotzero',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totltk','Dir Dtotltk',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtk','Dir Dtotmtk',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totltm','Dir Dtotltm',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtm','Dir Dtotmtm',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtg','Dir Dtotmtg',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtt','Dir Dtotmtt',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totsize','Dir Dtotsize',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minctime','Dir Dminctime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxctime','Dir Dmaxctime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minmtime','Dir Dminmtime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxmtime','Dir Dmaxmtime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minatime','Dir Dminatime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxatime','Dir Dmaxatime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mincrtime','Dir Dmincrtime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxcrtime','Dir Dmaxcrtime',1,0,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totxattr','Dir Dtotxattr',1,0,'int64');

insert into appformd values (NULL,'vrsummaryall',5,'DIR_pathname','Pathname',1,1,'text');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mode','Dir DMode',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_nlink','Dir Dnlink',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_uid','Dir Duid',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_gid','Dir Dgid',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_size','Dir Dsize',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mtime','Dir Dmtime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_ctime','Dir Dctime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_atime','Dir Datime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_linkname','Dir Dlinkname',1,1,'text');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totfiles','Dir Dtotfiles',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totlinks','Dir Dtotlinks',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minuid','Dir Dminuid',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxuid','Dir Dmaxuid',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mingid','Dir Dmingid',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxgid','Dir Dmaxgid',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minsize','Dir Dminsize',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxsize','Dir Dmaxsize',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totzero','Dir Dtotzero',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totltk','Dir Dtotltk',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtk','Dir Dtotmtk',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totltm','Dir Dtotltm',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtm','Dir Dtotmtm',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtg','Dir Dtotmtg',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totmtt','Dir Dtotmtt',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totsize','Dir Dtotsize',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minctime','Dir Dminctime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxctime','Dir Dmaxctime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minmtime','Dir Dminmtime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxmtime','Dir Dmaxmtime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_minatime','Dir Dminatime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_maxatime','Dir Dmaxatime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_mincrtime','Dir Dmincrtime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_dmaxcrtime','Dir Dmaxcrtime',1,1,'int64');
insert into appformd values (NULL,'vrsummaryall',5,'DIR_totxattr','Dir Dtotxattr',1,1,'int64');

-- you may want to put in your own path list for LOCALSEARCHSUBPATHn  you can have as many as you like
insert into appformdtree values (NULL,'vrsummaryall',1,'LOCALSEARCHSUBPATH1','');
insert into appformdtree values (NULL,'vrsummaryall',1,'LOCALSEARCHSUBPATH2','');
