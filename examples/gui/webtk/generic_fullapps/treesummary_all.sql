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



delete from apps where appname='treesummaryall';
delete from appsqueries where appsqname='treesummaryall';
delete from apppart where apppartname='treesummaryall';
delete from appforminfo where appfname='treesummaryall';
delete from appformd where appdname='treesummaryall';
delete from appformdtree where appdtname='treesummaryall';

-- you may want to change 'http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
insert into apps values (NULL,'treesummaryall','tree summaries (allvar) (local) with Traversal Control','Query of local treesummary Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'treesummaryall',tree summaries (allvar) (remote) with Traversal Control','Query of Remote treesummary Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
*/

insert into appsqueries values (NULL,'treesummaryall','V','1',1);
-- these can be added to run the gufi_query remotely both command to run like ssh/pdsh and the remote host
/* these statements are used for running remote gufi
insert into appsqueries values (NULL,'treesummaryall','R','ssh',1);
insert into appsqueries values (NULL,'treesummaryall','A','REMOTEHOST',1);
*/
insert into appsqueries values (NULL,'treesummaryall','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'treesummaryall','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'treesummaryall','J','insert into aggregate select {interimselected} from intermediate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'treesummaryall','G','select {interimselected} from aggregate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'treesummaryall','E','insert into intermediate select {selectonly} from treesummary inner join summary on treesummary.inode=summary.inode {where} {whereonly} {orderbyselected} {groupbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'treesummaryall','Z','select {displayselected} from vtmp {orderbyselected} {groupbyonly} {limitrec}',1);

insert into apppart values (NULL,'treesummaryall',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'treesummaryall',1,'treelist','Path Options','Optionally select search path','',1,2,0);
insert into apppart values (NULL,'treesummaryall',3,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
insert into apppart values (NULL,'treesummaryall',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'treesummaryall',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'treesummaryall',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'treesummaryall',7,'limit','limit results','Optionally limit results','',1,2,0);
insert into apppart values (NULL,'treesummaryall',8,'downloadf','download results in csv','download results csv','',1,2,0);

insert into appforminfo values (NULL,'treesummaryall',1,'textlist','treelist','treelist','treelist','treelist','Search Local Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');

insert into appforminfo values (NULL,'treesummaryall',3,'text','DIR_pathname','path()||''/''||summary.name as pathname','pathname','ipathname','Directory PathName','PathName of Directory',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'treesummaryall',3,'int64','DIR_depth','summary.depth as depth','depth','depth','Directory depth in tree','Directory depth in tree',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'treesummaryall',3,'int64','DIR_uid','uid as uid','uid','uid','Directory uid','Directory uid',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'treesummaryall',3,'int64','DIR_gid','gid as gid','gid','gid','Directory gid','Directory gid',1,1,0,'min=''0''','','','','','','','','','','W');

insert into appforminfo values (NULL,'treesummaryall',4,'text','DIR_pathname','path()||''/''||summary.name as pathname','pathname','pathname','Pathname','Name of tree',1,1,0,'required','checked','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totfiles','treesummary.totfiles as totfiles','totfiles','totfiles','tree totfiles ','totfiles of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totlinks','treesummary.totlinks as totlinks','totlinks','totlinks','tree totlinks ','totlinks of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_minuid','treesummary.minuid as minuid','minuid','minuid','tree minuid','minuid of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_maxuid','treesummary.maxuid as maxuid','maxuid','maxuid','tree maxuid','maxuid of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_mingid','tree.summary.mingid as mingid','mingid','mingid','tree mingid','mingid of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_maxgid','treesummary.maxgid as maxgid','maxgid','maxgid','tree maxgid','maxgid of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_minsize','treesummary.minsize as minsize','minsize','minsize','tree minsize','minsize of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_maxsize','treesummary.maxsize as maxsize','maxsize','maxsize','tree maxsize','maxsize of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totzero','treesummary.totzero as totzero','totzero','totzero','tree totzero','totzero of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totltk','treesummary.totltk as totltk','totltk','totltk','tree totltk','totltk of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totmtk','treesummary.totmtk as totmtk','totmtk','totmtk','tree totmtk','totmtk of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totltm','treesummary.totltm as totltm','totltm','totltm','tree totltm','totltm of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totmtm','treesummary.totmtm as totmtm','totmtm','totmtm','tree totmtm','totmtm of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totmtg','treesummary.totmtg as totmtg','totmtg','totmtg','tree totmtg','totmtg of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totmtt','treesummary.totmtt as totmtt','totmtt','totmtg','tree totmtt','totmtt of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totsize','treesummary.totsize as totsize','totsize','totsize','tree totsize','totsize of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_minctime','treesummary.minctime as minctime','minctime','datetime(minctime,"unixepoch") as dminctime','tree minctime','minctime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_maxctime','treesummary.maxctime as maxctime','maxctime','datetime(maxctime,"unixepoch") as dmaxctime','tree maxctime','maxctime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_minmtime','treesummary.minmtime as minmtime','minmtime','datetime(minmtime,"unixepoch") as dminmtime','tree minmtime','minctime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_maxmtime','treesummary.maxmtime as maxmtime','maxmtime','datetime(maxmtime,"unixepoch") as dmaxmtime','tree maxmtime','maxctime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_minatime','treesummary.minatime as minatime','minatime','datetime(minatime,"unixepoch") as dminatime','Dir minatime','minatime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_maxatime','treesummary.maxatime as maxatime','maxatime','datetime(maxatime,"unixepoch") as dmaxatime','Dir maxatime','maxatime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_mincrtime','treesummary.mincrtime as mincrtime','mincrtime','datetime(mincrtime,"unixepoch") as dmincrtime','tree mincrtime','mincrtime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_maxcrtime','treesummary.maxcrtime as maxcrtime','maxcrtime','datetime(maxcrtime,"unixepoch") as dmaxcrtime','tree maxcrtime','maxcrtime time of tree',1,1,1,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummaryall',4,'int64','DIR_totxattr','treesummary.totxattr as totxattr','totxattr','totxattr','tree totxattr','totxattr of tree',1,1,0,'','','','','','','','','','','S');

insert into appforminfo values (NULL,'treesummaryall',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');

insert into appforminfo values (NULL,'treesummaryall',5,'text','orderby2','orderby2','orderby2','orderby2','Second Order By','Second Order By',1,1,0,'','','','','','','','','','','O');
insert into appforminfo values (NULL,'treesummaryall',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');

insert into appforminfo values (NULL,'treesummaryall',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');

insert into appforminfo values (NULL,'treesummaryall',8,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');

insert into appformd values (NULL,'treesummaryall',5,'DIR_pathname','Pathname',1,0,'text');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totfiles','Dir Dtotfiles',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totlinks','Dir Dtotlinks',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minuid','Dir Dminuid',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxuid','Dir Dmaxuid',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_mingid','Dir Dmingid',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxgid','Dir Dmaxgid',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minsize','Dir Dminsize',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxsize','Dir Dmaxsize',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totzero','Dir Dtotzero',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totltk','Dir Dtotltk',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtk','Dir Dtotmtk',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totltm','Dir Dtotltm',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtm','Dir Dtotmtm',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtg','Dir Dtotmtg',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtt','Dir Dtotmtt',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totsize','Dir Dtotsize',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minctime','Dir Dminctime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxctime','Dir Dmaxctime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minmtime','Dir Dminmtime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxmtime','Dir Dmaxmtime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minatime','Dir Dminatime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxatime','Dir Dmaxatime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_mincrtime','Dir Dmincrtime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxcrtime','Dir Dmaxcrtime',1,0,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totxattr','Dir Dtotxattr',1,0,'int64');

insert into appformd values (NULL,'treesummaryall',5,'DIR_pathname','Pathname',1,1,'text');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totfiles','Dir Dtotfiles',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totlinks','Dir Dtotlinks',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minuid','Dir Dminuid',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxuid','Dir Dmaxuid',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_mingid','Dir Dmingid',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxgid','Dir Dmaxgid',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minsize','Dir Dminsize',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxsize','Dir Dmaxsize',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totzero','Dir Dtotzero',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totltk','Dir Dtotltk',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtk','Dir Dtotmtk',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totltm','Dir Dtotltm',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtm','Dir Dtotmtm',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtg','Dir Dtotmtg',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totmtt','Dir Dtotmtt',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totsize','Dir Dtotsize',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minctime','Dir Dminctime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxctime','Dir Dmaxctime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minmtime','Dir Dminmtime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxmtime','Dir Dmaxmtime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_minatime','Dir Dminatime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_maxatime','Dir Dmaxatime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_mincrtime','Dir Dmincrtime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_dmaxcrtime','Dir Dmaxcrtime',1,1,'int64');
insert into appformd values (NULL,'treesummaryall',5,'DIR_totxattr','Dir Dtotxattr',1,1,'int64');

-- you may want to put in your own path list for LOCALSEARCHSUBPATHn  you can have as many as you like
insert into appformdtree values (NULL,'treesummaryall',1,'LOCALSEARCHSUBPATH1','');
insert into appformdtree values (NULL,'treesummaryall',1,'LOCALSEARCHSUBPATH2','');
