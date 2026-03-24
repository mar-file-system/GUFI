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



delete from apps where appname='treesummarysimple';
delete from appsqueries where appsqname='treesummarysimple';
delete from apppart where apppartname='treesummarysimple';
delete from appforminfo where appfname='treesummarysimple';
delete from appformd where appdname='treesummarysimple';
delete from appformdtree where appdtname='treesummarysimple';

-- you may want to change 'http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
insert into apps values (NULL,'treesummarysimple','tree summaries with Traversal Control','Treesummary Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'treesummarysimple',tree summaries (allvar) (remote) with Traversal Control','Query of Remote treesummary Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
*/

insert into appsqueries values (NULL,'treesummarysimple','V','1',1);
-- these can be added to run the gufi_query remotely both command to run like ssh/pdsh and the remote host
/* these statements are used for running remote gufi
insert into appsqueries values (NULL,'treesummarysimple','R','ssh',1);
insert into appsqueries values (NULL,'treesummarysimple','A','REMOTEHOST',1);
*/
insert into appsqueries values (NULL,'treesummarysimple','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'treesummarysimple','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'treesummarysimple','J','insert into aggregate select {interimselected} from intermediate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'treesummarysimple','G','select {interimselected} from aggregate {orderbyselected} {limitrec}',1);
insert into appsqueries values (NULL,'treesummarysimple','E','insert into intermediate select {selectonly} from treesummary inner join summary on treesummary.inode=summary.inode {where} {whereonly} {orderbyselected} {groupbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'treesummarysimple','Z','select {displayselected} from vtmp {orderbyselected} {groupbyonly} {limitrec}',1);

insert into apppart values (NULL,'treesummarysimple',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'treesummarysimple',3,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
insert into apppart values (NULL,'treesummarysimple',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'treesummarysimple',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'treesummarysimple',7,'limit','limit results','Optionally limit results','',1,2,0);

insert into appforminfo values (NULL,'treesummarysimple',1,'textlist','treelist','treelist','treelist','treelist','Search Local Tree','Path to Search Tree',1,2,0,'','','','','','','','','','','T');

insert into appforminfo values (NULL,'treesummarysimple',3,'text','DIR_pathname','path()||''/''||summary.name as pathname','pathname','ipathname','Directory PathName','PathName of Directory',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'treesummarysimple',3,'int64','DIR_depth','summary.depth as depth','depth','depth','Directory depth in tree','Directory depth in tree',1,1,0,'min=''0''','','','','','','','','','','W');

insert into appforminfo values (NULL,'treesummarysimple',4,'text','DIR_pathname','path()||''/''||summary.name as pathname','pathname','pathname','Pathname','Name of tree',1,1,0,'required','checked','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummarysimple',4,'int64','DIR_totfiles','treesummary.totfiles as totfiles','totfiles','totfiles','tree totfiles ','totfiles of tree',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'treesummarysimple',4,'int64','DIR_totsize','treesummary.totsize as totsize','totsize','totsize','tree totsize','totsize of tree',1,1,0,'','','','','','','','','','','S');

insert into appforminfo values (NULL,'treesummarysimple',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');

insert into appforminfo values (NULL,'treesummarysimple',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');

insert into appforminfo values (NULL,'treesummarysimple',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');

insert into appformd values (NULL,'treesummarysimple',5,'DIR_totfiles','Dir Dtotfiles',1,0,'int64');
insert into appformd values (NULL,'treesummarysimple',5,'DIR_totsize','Dir Dtotsize',1,0,'int64');
