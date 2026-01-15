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



PRAGMA foreign_keys=OFF;

BEGIN TRANSACTION;

CREATE TABLE global (id integer primary key,rowcount int64, rowmax int64);
INSERT INTO global VALUES(1,1,50);

CREATE TABLE apps (id integer primary key,appname text, apptitle text, apptitletext text, appurl text,appdebug int64,appselwhereinputrequired int64,appvt text,appthreads int64,appsearchtree text, appsourcetree text,appurlsourcepath text);
INSERT INTO apps VALUES(1,'gufibigentrieslocaltraversal','Local File Entries Query with Traversal Control','Query of Local File Entries Information with Traversal Control','',1,1,'gufi_vt',30,'<index path>','','');
INSERT INTO apps VALUES(2,'gufibigentriesremotetraversal','Remote File Entries Query with Traversal Control','Query of Remote File Entries Information with Traversal Control','',1,1,'gufi_vt',30,'<remote index path>','','');
INSERT INTO apps VALUES(3,'gufibiggrpbysummary','Group by on Directory Attributes','Group by on Directory Attributes','',1,1,'gufi_vt',1,'<index path>','','');

CREATE TABLE appsqueries (id integer primary key,appsqname text, appsqtype text, appsquery text, appsqorder int64 );
INSERT INTO appsqueries VALUES(1,'gufibigentrieslocaltraversal','V','1',1);
INSERT INTO appsqueries VALUES(2,'gufibigentrieslocaltraversal','I','create table intermediate ({interimcreateselected})',1);
INSERT INTO appsqueries VALUES(3,'gufibigentrieslocaltraversal','K','create table aggregate ({interimcreateselected})',1);
INSERT INTO appsqueries VALUES(4,'gufibigentrieslocaltraversal','J','insert into aggregate select {interimselected} from intermediate {orderbyselected} {limitrec}',1);
INSERT INTO appsqueries VALUES(5,'gufibigentrieslocaltraversal','G','select {interimselected} from aggregate {orderbyselected} {limitrec}',1);
INSERT INTO appsqueries VALUES(6,'gufibigentrieslocaltraversal','E','insert into intermediate select {selectonly} from vrxpentries {where} {whereonly} {orderbyselected} {groupbyonly} {limitrec}',1);
INSERT INTO appsqueries VALUES(7,'gufibigentrieslocaltraversal','Z','select {displayselected} from vtmp {orderbyselected} {groupbyonly} {limitrec}',1);
INSERT INTO appsqueries VALUES(8,'gufibigentriesremotetraversal','V','1',1);
INSERT INTO appsqueries VALUES(9,'gufibigentriesremotetraversal','R','ssh',1);
INSERT INTO appsqueries VALUES(10,'gufibigentriesremotetraversal','A','<remote>',1);
INSERT INTO appsqueries VALUES(11,'gufibigentriesremotetraversal','I','create table intermediate ({interimcreateselected})',1);
INSERT INTO appsqueries VALUES(12,'gufibigentriesremotetraversal','K','create table aggregate ({interimcreateselected})',1);
INSERT INTO appsqueries VALUES(13,'gufibigentriesremotetraversal','J','insert into aggregate select {interimselected} from intermediate {orderbyselected} {limitrec}',1);
INSERT INTO appsqueries VALUES(14,'gufibigentriesremotetraversal','G','select {interimselected} from aggregate {orderbyselected} {limitrec}',1);
INSERT INTO appsqueries VALUES(15,'gufibigentriesremotetraversal','E','insert into intermediate select {selectonly} from vrxpentries {where} {whereonly} {orderbyselected} {groupbyonly} {limitrec}',1);
INSERT INTO appsqueries VALUES(16,'gufibigentriesremotetraversal','Z','select {displayselected} from vtmp {orderbyselected} {groupbyonly} {limitrec}',1);
INSERT INTO appsqueries VALUES(17,'gufibiggrpbysummary','I','create table intermediate ({interimcreateselected})',1);
INSERT INTO appsqueries VALUES(18,'gufibiggrpbysummary','K','create table aggregate ({interimcreateselected})',1);
INSERT INTO appsqueries VALUES(19,'gufibiggrpbysummary','S','insert into intermediate select {selectonly} from vrsummary {where} {whereonly} {groupbyonly} {orderbyonly} {limitrec}',1);
INSERT INTO appsqueries VALUES(20,'gufibiggrpbysummary','J','insert into aggregate select {interimselected} from intermediate',1);
INSERT INTO appsqueries VALUES(21,'gufibiggrpbysummary','G','select {interimselected} from aggregate',1);
INSERT INTO appsqueries VALUES(22,'gufibiggrpbysummary','Z','select {displayselected} from vtmp {groupbyonly} {orderbyonly} {limitrec}',1);

CREATE TABLE apppart (id integer primary key,apppartname text,apppartnum int64,appparttype text,apppartheading text,apppartheadingitext text,apppartquery text,appparttablewidth int64,apppartlevel int64,apppartrequired int64);
INSERT INTO apppart VALUES(1,'gufibigentrieslocaltraversal',0,'travcontrol','Local Traversal Control','Optionally specify traversal control','',1,2,0);
INSERT INTO apppart VALUES(2,'gufibigentrieslocaltraversal',1,'input','File Input Criteria','Please enter at least one','',6,2,1);
INSERT INTO apppart VALUES(3,'gufibigentrieslocaltraversal',2,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
INSERT INTO apppart VALUES(4,'gufibigentrieslocaltraversal',3,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
INSERT INTO apppart VALUES(5,'gufibigentrieslocaltraversal',4,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
INSERT INTO apppart VALUES(6,'gufibigentrieslocaltraversal',5,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
INSERT INTO apppart VALUES(7,'gufibigentrieslocaltraversal',6,'limit','limit results','Optionally limit results','',1,2,0);
INSERT INTO apppart VALUES(8,'gufibigentrieslocaltraversal',7,'downloadf','download results in csv','download results csv','',1,2,0);
INSERT INTO apppart VALUES(9,'gufibigentriesremotetraversal',0,'travcontrol','Remote Traversal Control','Optionally specify traversal control','',1,2,0);
INSERT INTO apppart VALUES(10,'gufibigentriesremotetraversal',1,'input','File Input Criteria','Please enter at least one','',6,2,1);
INSERT INTO apppart VALUES(11,'gufibigentriesremotetraversal',2,'input','Within Directory Input Criteria','Optionally enter','',6,2,0);
INSERT INTO apppart VALUES(12,'gufibigentriesremotetraversal',3,'checkbox','Select Fields to retieve','Please check at least one','',12,2,1);
INSERT INTO apppart VALUES(13,'gufibigentriesremotetraversal',4,'orderby','Order by','Optionally Order by by, you must select a variable in the checkboxes above in order to use it in an order by','',2,2,0);
INSERT INTO apppart VALUES(14,'gufibigentriesremotetraversal',5,'groupby','Group by','Optionally Group by , you must select a variable in the checkboxes above in order to use it in an group by','',2,2,0);
INSERT INTO apppart VALUES(15,'gufibigentriesremotetraversal',6,'limit','limit results','Optionally limit results','',1,2,0);
INSERT INTO apppart VALUES(16,'gufibigentriesremotetraversal',7,'downloadf','download results in csv','download results csv','',1,2,0);
INSERT INTO apppart VALUES(17,'gufibiggrpbysummary',0,'tree','Search Location','Optionally select search path','',1,2,0);
INSERT INTO apppart VALUES(18,'gufibiggrpbysummary',1,'input','Input Criteria','Please enter at least one','',4,2,1);
INSERT INTO apppart VALUES(19,'gufibiggrpbysummary',2,'checkbox','Select categories','Please check at least one','',4,2,1);
INSERT INTO apppart VALUES(20,'gufibiggrpbysummary',3,'orderby','Order by','Optionally Order by','',2,2,0);
INSERT INTO apppart VALUES(21,'gufibiggrpbysummary',4,'groupby','Group by','Optionally Group by','',2,2,0);
INSERT INTO apppart VALUES(22,'gufibiggrpbysummary',5,'limit','limit results','Optionally limit results','',1,2,0);

CREATE TABLE appforminfo (id integer primary key,appfname text, appfpartnum int64, appftype text,appfjoin text, appfselectnm text,appfinterimnm text, appfdisplaynm text, appfshortnm text, appflongnm text, appforder int64, appfdiff int64,appfitype int64,appfv0 text,appfv1 text,appfv2 text,appfv3 text,appfv4,appfv5 text,preselect text,postselect text,prewhere text,postwhere text,appfparttype text);
INSERT INTO appforminfo VALUES(1,'gufibigentrieslocaltraversal',1,'text','ENT_path','rpath(sname,sroll) as path','path','path','Path','Path of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(2,'gufibigentrieslocaltraversal',1,'text','ENT_name','name as name','name','name','File Name','Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(3,'gufibigentrieslocaltraversal',1,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(4,'gufibigentrieslocaltraversal',1,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(5,'gufibigentrieslocaltraversal',1,'int64','ENT_size.1','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(6,'gufibigentrieslocaltraversal',1,'int64','ENT_size.2','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(7,'gufibigentrieslocaltraversal',1,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'pattern=''[A-Za-z]+''','placeholder=''%str%''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(8,'gufibigentrieslocaltraversal',1,'int64','ENT_uid','uid as uid','uid','uid','File UID','File UID',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(9,'gufibigentrieslocaltraversal',1,'int64','ENT_gid','gid as gid','gid','gid','File GID','File GID',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(10,'gufibigentrieslocaltraversal',1,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(11,'gufibigentrieslocaltraversal',1,'int64','ENT_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(12,'gufibigentrieslocaltraversal',1,'int64','ENT_mtime.2','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(13,'gufibigentrieslocaltraversal',1,'int64','ENT_ctime.1','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(14,'gufibigentrieslocaltraversal',1,'int64','ENT_ctime.2','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(15,'gufibigentrieslocaltraversal',2,'int64','DIR_dmode','dmode as dmode','dmode','modetotxt(dmode) as dmode','Directory Mode','Mode of Directory',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(16,'gufibigentrieslocaltraversal',2,'int64','DIR_dtotfiles','dtotfiles as dtotfiles','dtotfiles','dtotfiles','Directory total files','Directory total files',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(17,'gufibigentrieslocaltraversal',2,'int64','DIR_dtotlinks','dtotlinks as dtotlinks','dtotlinks','dtotlinks','Directory total links','Directory total links',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(18,'gufibigentrieslocaltraversal',2,'int64','DIR_dminsize','dminsize as dminsize','dminsize','dminsize','Directory min filesize','Directory min filesize',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(19,'gufibigentrieslocaltraversal',2,'int64','DIR_dmaxsize','dmaxsize as dmaxsize','dmaxsize','dmaxsize','Directory max filesize','Directory max filesize',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(20,'gufibigentrieslocaltraversal',2,'int64','DIR_dtotzero','dtotzero as dtotzero','dtotzeroe','dtotzero','Directory total zero files','Directory total zero files',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(21,'gufibigentrieslocaltraversal',3,'text','ENT_pathname','rpath(sname,sroll)||''/''||name as pathname','pathname','pathname','Pathname','Name of File',1,1,0,'required','checked','','','','','','','','','S');
INSERT INTO appforminfo VALUES(22,'gufibigentrieslocaltraversal',3,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(23,'gufibigentrieslocaltraversal',3,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(24,'gufibigentrieslocaltraversal',3,'int64','ENT_size','size as size','size','size','File Size','Size of File',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(25,'gufibigentrieslocaltraversal',3,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(26,'gufibigentrieslocaltraversal',3,'int64','ENT_uid','uid as uid','uid','uidtouser(uid) as localuser','File UID','File UID',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(27,'gufibigentrieslocaltraversal',3,'int64','ENT_gid','gid as gid','gid','gidtogroup(gid) as localgroup','File GID','File GID',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(28,'gufibigentrieslocaltraversal',3,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(29,'gufibigentrieslocaltraversal',3,'int64','ENT_mtime','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(30,'gufibigentrieslocaltraversal',3,'int64','ENT_ctime','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(31,'gufibigentrieslocaltraversal',4,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');
INSERT INTO appforminfo VALUES(32,'gufibigentrieslocaltraversal',4,'text','orderby2','orderby2','orderby2','orderby2','Second Order By','Second Order By',1,1,0,'','','','','','','','','','','O');
INSERT INTO appforminfo VALUES(33,'gufibigentrieslocaltraversal',5,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');
INSERT INTO appforminfo VALUES(34,'gufibigentrieslocaltraversal',6,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');
INSERT INTO appforminfo VALUES(35,'gufibigentrieslocaltraversal',7,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');
INSERT INTO appforminfo VALUES(36,'gufibigentriesremotetraversal',1,'text','ENT_path','rpath(sname,sroll) as path','path','path','Path','Path of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(37,'gufibigentriesremotetraversal',1,'text','ENT_name','name as name','name','name','File Name','Name of File',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(38,'gufibigentriesremotetraversal',1,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(39,'gufibigentriesremotetraversal',1,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'pattern=''[A-Za-z,&#39;%._]+''','placeholder=''%str% or "str1","str2" for in''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(40,'gufibigentriesremotetraversal',1,'int64','ENT_size.1','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(41,'gufibigentriesremotetraversal',1,'int64','ENT_size.2','size as size','size','size','File Size','Size of File',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(42,'gufibigentriesremotetraversal',1,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'pattern=''[A-Za-z]+''','placeholder=''%str%''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(43,'gufibigentriesremotetraversal',1,'int64','ENT_uid','uid as uid','uid','uid','File UID','File UID',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(44,'gufibigentriesremotetraversal',1,'int64','ENT_gid','gid as gid','gid','gid','File GID','File GID',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(45,'gufibigentriesremotetraversal',1,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(46,'gufibigentriesremotetraversal',1,'int64','ENT_mtime.1','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(47,'gufibigentriesremotetraversal',1,'int64','ENT_mtime.2','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(48,'gufibigentriesremotetraversal',1,'int64','ENT_ctime.1','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(49,'gufibigentriesremotetraversal',1,'int64','ENT_ctime.2','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(50,'gufibigentriesremotetraversal',2,'int64','DIR_dmode','dmode as dmode','dmode','modetotxt(dmode) as dmode','Directory Mode','Mode of Directory',1,1,2,'min=''0''','placeholder=''octal mask''','','','','','','','','','W');
INSERT INTO appforminfo VALUES(51,'gufibigentriesremotetraversal',2,'int64','DIR_dtotfiles','dtotfiles as dtotfiles','dtotfiles','dtotfiles','Directory total files','Directory total files',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(52,'gufibigentriesremotetraversal',2,'int64','DIR_dtotlinks','dtotlinks as dtotlinks','dtotlinks','dtotlinks','Directory total links','Directory total links',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(53,'gufibigentriesremotetraversal',2,'int64','DIR_dminsize','dminsize as dminsize','dminsize','dminsize','Directory min filesize','Directory min filesize',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(54,'gufibigentriesremotetraversal',2,'int64','DIR_dmaxsize','dmaxsize as dmaxsize','dmaxsize','dmaxsize','Directory max filesize','Directory max filesize',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(55,'gufibigentriesremotetraversal',2,'int64','DIR_dtotzero','dtotzero as dtotzero','dtotzeroe','dtotzero','Directory total zero files','Directory total zero files',1,1,0,'min=''0''','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(56,'gufibigentriesremotetraversal',3,'text','ENT_pathname','rpath(sname,sroll)||''/''||name as pathname','pathname','pathname','Pathname','Name of File',1,1,0,'required','checked','','','','','','','','','S');
INSERT INTO appforminfo VALUES(57,'gufibigentriesremotetraversal',3,'text','ENT_xattr_name','xattr_name as xattr_name','xattr_name','xattr_name','xattrname','xattrname',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(58,'gufibigentriesremotetraversal',3,'text','ENT_xattr_value','xattr_value as xattr_value','xattr_value','xattr_value','xattrvalue','xattrvalue',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(59,'gufibigentriesremotetraversal',3,'int64','ENT_size','size as size','size','size','File Size','Size of File',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(60,'gufibigentriesremotetraversal',3,'text','ENT_type','type as type','type','type','Entry Type','Type of Entry',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(61,'gufibigentriesremotetraversal',3,'int64','ENT_uid','uid as uid','uid','uidtouser(uid) as localuser','File UID','File UID',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(62,'gufibigentriesremotetraversal',3,'int64','ENT_gid','gid as gid','gid','gidtogroup(gid) as localgroup','File GID','File GID',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(63,'gufibigentriesremotetraversal',3,'int64','ENT_mode','mode as mode','mode','modetotxt(mode) as mode','File Mode','Mode of File',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(64,'gufibigentriesremotetraversal',3,'int64','ENT_mtime','mtime as mtime','mtime','datetime(mtime,"unixepoch") as mtime','File Mod Time','Mod time of File',1,1,1,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(65,'gufibigentriesremotetraversal',3,'int64','ENT_ctime','ctime as ctime','ctime','datetime(ctime,"unixepoch") as ctime','File Change Time','Change time of File',1,1,1,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(66,'gufibigentriesremotetraversal',4,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');
INSERT INTO appforminfo VALUES(67,'gufibigentriesremotetraversal',4,'text','orderby2','orderby2','orderby2','orderby2','Second Order By','Second Order By',1,1,0,'','','','','','','','','','','O');
INSERT INTO appforminfo VALUES(68,'gufibigentriesremotetraversal',5,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');
INSERT INTO appforminfo VALUES(69,'gufibigentriesremotetraversal',6,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'min=''0''','','','','','','','','','','L');
INSERT INTO appforminfo VALUES(70,'gufibigentriesremotetraversal',7,'text','downloadf','downloadf','downloadf','downloadf','Download results','Download Results',1,1,0,'','','','','','','','','','','D');
INSERT INTO appforminfo VALUES(71,'gufibiggrpbysummary',0,'text','tree','tree','tree','tree','Search Tree','Path to Search Tree',1,1,0,'','','','','','','','','','','T');
INSERT INTO appforminfo VALUES(72,'gufibiggrpbysummary',1,'text','path','path() as path','path','path','Path','Path of Directory',1,1,0,'','','','','','','','','','','W');
INSERT INTO appforminfo VALUES(73,'gufibiggrpbysummary',2,'int64','totdirs','count(name) as totdirs','totdirs','sum(totdirs) as totdirs','Count of directories','Count of directories',1,1,0,'required','checked','','','','','','','','','S');
INSERT INTO appforminfo VALUES(74,'gufibiggrpbysummary',2,'int64','totfiles','totfiles as totfiles','totfiles','sum(totfiles) as totfiles','Count of files','Count of Files',1,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(75,'gufibiggrpbysummary',2,'int64','totsize','totsize as totsize','totsize','sum(totsize) as totsize','Sum of file size','Sum of File Size',2,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(76,'gufibiggrpbysummary',2,'text','totfilesize','case when totsize<1 then "Zero" when totsize<1024 then "1-K" when totsize>1023 and totsize<1024*1024 then "K-M" when totsize>1024*1024-1 and totsize<1024*1024*1024 then "M-G" when totsize>1024*1024*1024-1 and totsize<1024*1024*1024*1024 then "G-T" else "GT1T" end as totfilesize','totfilesize','totfilesize','total file size category','total file size category',2,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(77,'gufibiggrpbysummary',2,'text','maxfileage','case when unixepoch("now")-minmtime<86400 then "0D-1D" when unixepoch("now")-minmtime>86400-1 and unixepoch("now")-minmtime<86400*7 then "1D-1W" when unixepoch("now")-minmtime>86400*7-1 and unixepoch("now")-minmtime<86400*30 then "1W-1M" when unixepoch("now")-minmtime>86400*30-1 and unixepoch("now")-minmtime<86400*365 then "1M-1Y" when unixepoch("now")-minmtime>86400*365-1 and unixepoch("now")-minmtime<86400*365*2 then "1-2Y" when unixepoch("now")-minmtime>86400*365*2-1  and unixepoch("now")-minmtime<86400*365*5 then "2-5Y" when unixepoch("now")-minmtime>86400*365*5-1  and unixepoch("now")-minmtime<86400*365*10 then "5-10y" when unixepoch("now")-minmtime>86400*365*10-1  and unixepoch("now")-minmtime<86400*365*20 then "10-20Y" else "GT20Y" end as maxfileage','maxfileage','maxfileage','max file age category','max file age category',2,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(78,'gufibiggrpbysummary',2,'text','fsizedist','case when totltk>0 then "ltk" else "___" end||case when totmtk>0 then "mtk" else "___" end||case when totltm>0 then "ltm" else "___" end||case when totmtm>0 then "mtm" else "___" end||case when totmtg>0 then "mtg" else "___" end||case when totmtt>0 then "mtt" else "___" end as fsizedist','fsizedist','fsizedist','file size distro','file size distro',2,1,0,'','','','','','','','','','','S');
INSERT INTO appforminfo VALUES(79,'gufibiggrpbysummary',3,'text','orderby1','orderby1','orderby1','orderby1','First Order By','First Order By',1,1,0,'','','','','','','','','','','O');
INSERT INTO appforminfo VALUES(80,'gufibiggrpbysummary',4,'text','groupby1','groupby1','groupby1','groupby1','Group By','Group By',1,1,0,'','','','','','','','','','','G');
INSERT INTO appforminfo VALUES(81,'gufibiggrpbysummary',5,'text','limit','limit','limit','limit','Limit results','Limit Results',1,1,0,'','','','','','','','','','','L');

CREATE TABLE appformd (id integer primary key,appdname text, appdpartnum int64, appdinterimnm text, appdshortnm text, appdorder int64,appddiff int64,appdtp text);
INSERT INTO appformd VALUES(1,'gufibigentrieslocaltraversal',4,'ENT_pathname','Pathname',1,0,'text');
INSERT INTO appformd VALUES(2,'gufibigentrieslocaltraversal',4,'ENT_size','File Size',1,0,'int64');
INSERT INTO appformd VALUES(3,'gufibigentrieslocaltraversal',4,'ENT_mode','File Mode',1,0,'int64');
INSERT INTO appformd VALUES(4,'gufibigentrieslocaltraversal',4,'ENT_xattr_name','Xattr Name',1,0,'text');
INSERT INTO appformd VALUES(5,'gufibigentrieslocaltraversal',4,'ENT_xattr_value','Xattr Value',1,0,'text');
INSERT INTO appformd VALUES(6,'gufibigentrieslocaltraversal',4,'ENT_mtime','File Mtime',1,0,'int64');
INSERT INTO appformd VALUES(7,'gufibigentrieslocaltraversal',4,'ENT_ctime','File Ctime',1,0,'int64');
INSERT INTO appformd VALUES(8,'gufibigentrieslocaltraversal',4,'ENT_size','File Size',1,1,'int64');
INSERT INTO appformd VALUES(9,'gufibigentrieslocaltraversal',4,'ENT_mode','File Mode',1,1,'int64');
INSERT INTO appformd VALUES(10,'gufibigentrieslocaltraversal',4,'ENT_xattr_name','Xattr Name',1,1,'text');
INSERT INTO appformd VALUES(11,'gufibigentrieslocaltraversal',4,'ENT_xattr_value','Xattr Value',1,1,'text');
INSERT INTO appformd VALUES(12,'gufibigentrieslocaltraversal',4,'ENT_mtime','File Mtime',1,1,'int64');
INSERT INTO appformd VALUES(13,'gufibigentrieslocaltraversal',4,'ENT_ctime','File Ctime',1,1,'int64');
INSERT INTO appformd VALUES(14,'gufibigentriesremotetraversal',4,'ENT_pathname','Pathname',1,0,'text');
INSERT INTO appformd VALUES(15,'gufibigentriesremotetraversal',4,'ENT_size','File Size',1,0,'int64');
INSERT INTO appformd VALUES(16,'gufibigentriesremotetraversal',4,'ENT_mode','File Mode',1,0,'int64');
INSERT INTO appformd VALUES(17,'gufibigentriesremotetraversal',4,'ENT_xattr_name','Xattr Name',1,0,'text');
INSERT INTO appformd VALUES(18,'gufibigentriesremotetraversal',4,'ENT_xattr_value','Xattr Value',1,0,'text');
INSERT INTO appformd VALUES(19,'gufibigentriesremotetraversal',4,'ENT_mtime','File Mtime',1,0,'int64');
INSERT INTO appformd VALUES(20,'gufibigentriesremotetraversal',4,'ENT_ctime','File Ctime',1,0,'int64');
INSERT INTO appformd VALUES(21,'gufibigentriesremotetraversal',4,'ENT_size','File Size',1,1,'int64');
INSERT INTO appformd VALUES(22,'gufibigentriesremotetraversal',4,'ENT_mode','File Mode',1,1,'int64');
INSERT INTO appformd VALUES(23,'gufibigentriesremotetraversal',4,'ENT_xattr_name','Xattr Name',1,1,'text');
INSERT INTO appformd VALUES(24,'gufibigentriesremotetraversal',4,'ENT_xattr_value','Xattr Value',1,1,'text');
INSERT INTO appformd VALUES(25,'gufibigentriesremotetraversal',4,'ENT_mtime','File Mtime',1,1,'int64');
INSERT INTO appformd VALUES(26,'gufibigentriesremotetraversal',4,'ENT_ctime','File Ctime',1,1,'int64');
INSERT INTO appformd VALUES(27,'gufibiggrpbysummary',3,'totsize','total sum of file size',1,0,'int64');
INSERT INTO appformd VALUES(28,'gufibiggrpbysummary',3,'totfiles','counttotal count of files',1,0,'int64');
INSERT INTO appformd VALUES(29,'gufibiggrpbysummary',3,'totdirs','counttotal count of directories',1,0,'int64');
INSERT INTO appformd VALUES(30,'gufibiggrpbysummary',4,'totfilesize','file size category',1,0,'text');
INSERT INTO appformd VALUES(31,'gufibiggrpbysummary',4,'maxfileage','file age category',1,0,'text');
INSERT INTO appformd VALUES(32,'gufibiggrpbysummary',4,'fsizedist','file size distro',1,0,'text');

CREATE TABLE appformdtree (id integer primary key,appdtname text, appdtpartnum int64, appdtparttreeitem text, appdtpartparm text);
/* INSERT INTO appformdtree VALUES(1,'gufibigentrieslocaltraversal',0,'<index path>',''); */
/* INSERT INTO appformdtree VALUES(2,'gufibigentriesremotetraversal',0,'<remote index path>',''); */

COMMIT;
