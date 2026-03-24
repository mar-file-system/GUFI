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



delete from apps where appname='vrxpentriessimplegroupby';
delete from appsqueries where appsqname='vrxpentriessimplegroupby';
delete from apppart where apppartname='vrxpentriessimplegroupby';
delete from appforminfo where appfname='vrxpentriessimplegroupby';
delete from appformd where appdname='vrxpentriessimplegroupby';
delete from appformdtree where appdtname='vrxpentriessimplegroupby';

-- you may want to change 'http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
insert into apps values (NULL,'vrxpentriessimplegroupby','Files Group By with Traversal Control','Files groupby Information with Traversal Control','http://localhost:8001',1,1,'gufi_vt',30,'/LOCALSEARCHPATH','/LOCALSOURCEPATH','http://localhost:8000/search');
-- this is for remote gufi instead of the above local gufi
/*
insert into apps values (NULL,'vrxpentriessimplegroupby',files groupby (allvar) (remote) with Traversal Control','Query of Remote File Entries groupby Information with Traversal Control','http://localhost:8501',1,1,'gufi_vt',30,'/REMOTESEARCHPATH','/REMOTESOURCEPATH','http://localhost:8000/search');
*/

insert into appsqueries values (NULL,'vrxpentriessimplegroupby','V','1',1);
-- these can be added to run the gufi_query remotely both command to run like ssh/pdsh and the remote host
/* these statements are used for running remote gufi
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','R','ssh',1);
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','A','REMOTEHOST',1);
*/
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','I','create table intermediate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','K','create table aggregate ({interimcreateselected})',1);
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','J','insert into aggregate select {interimselected} from intermediate',1);
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','G','select {interimselected} from aggregate' ,1);
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','E','insert into intermediate select {selectonly} from vrxpentries {where} {whereonly} {groupbyonly} {orderbyonly} {limitrec}',1);
insert into appsqueries values (NULL,'vrxpentriessimplegroupby','Z','select {displayselected} from vtmp {groupbyonly} {orderbyonly} {limitrec}',1);


insert into apppart values (NULL,'vrxpentriessimplegroupby',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
insert into apppart values (NULL,'vrxpentriessimplegroupby',2,'input','File Input Criteria','Please enter at least one','',12,2,1);
insert into apppart values (NULL,'vrxpentriessimplegroupby',4,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
insert into apppart values (NULL,'vrxpentriessimplegroupby',5,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
insert into apppart values (NULL,'vrxpentriessimplegroupby',6,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
insert into apppart values (NULL,'vrxpentriessimplegroupby',7,'limit','limit results','Optionally limit results','',1,2,0);


insert into appforminfo values (NULL,'vrxpentriessimplegroupby',2,'text','ENT_path','rpath(sname,sroll) as path','path','path','Path','Path of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriessimplegroupby',2,'text','ENT_name','name as name','name','name','File Name','Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriessimplegroupby',2,'int64','ENT_size.1','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
insert into appforminfo values (NULL,'vrxpentriessimplegroupby',2,'int64','ENT_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');

--- since this is groupby for vrxpentries (essentiall its the entries part only --
--- the -ikj queries will be more like a group by where we sum the counts as we traverse --
--- there will be a groupby for summary that will cover all the summary parts --

insert into appforminfo values (NULL,'vrxpentriessimplegroupby',4,'int64','ENT_inode_cnt','count(inode) as inode_cnt','inode_cnt','sum(inode_cnt) as inode_cnt  ','Count of files','Count of Files',1,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriessimplegroupby',4,'int64','ENT_size_sum','sum(size) as size_sum','size_sum','sum(size_sum) as size_sum','Sum of file size','Sum of File Size',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriessimplegroupby',4,'text','ENT_ext_cat','ext(name) as ext_cat','ext_cat','ext_cat','file extention category','file extension category',2,1,0,'','','','','','','','','','','S');

insert into appforminfo values (NULL,'vrxpentriessimplegroupby',4,'text','ENT_size_cat','bytecat(size) as size_cat','size_cat','size_cat','size category','size category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriessimplegroupby',4,'text','ENT_mtime_cat','agecat(mtime) as mtime_cat','mtime_cat','mtime_cat','mtime category','mtime category',2,1,0,'','','','','','','','','','','S');
insert into appforminfo values (NULL,'vrxpentriessimplegroupby',5,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');

insert into appforminfo values (NULL,'vrxpentriessimplegroupby',6,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');

insert into appforminfo values (NULL,'vrxpentriessimplegroupby',7,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');

insert into appforminfo values (NULL,'vrxpentriessimplegroupby',8,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');


--- appformd values need to be same as select values
insert into appformd values (NULL,'vrxpentriessimplegroupby',5,'ENT_size_sum','File Size',1,0,'int64');
insert into appformd values (NULL,'vrxpentriessimplegroupby',5,'ENT_inode_cnt','inode cnt',1,0,'int64');

---- need appformd for groupby
insert into appformd values (NULL,'vrxpentriessimplegroupby',6,'ENT_size_cat','size category',1,0,'text');
insert into appformd values (NULL,'vrxpentriessimplegroupby',6,'ENT_mtime_cat','mtime category',1,0,'text');
insert into appformd values (NULL,'vrxpentriessimplegroupby',6,'ENT_ext_cat','ext category',1,0,'text');
