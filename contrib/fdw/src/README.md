# GUFI Foreign Data Wrapper

This directory contains code implementing a PostgreSQL Foreign Data
Wrapper for GUFI using the
[Supabase API](https://github.com/supabase/wrappers).  Because the
code is built off of examples from the repository, the contents of
this directory is licensed under the [Apache-2.0
license](https://github.com/supabase/wrappers/blob/main/LICENSE).

# Usage
## Set Up
### supabase/wrappers
See [README](https://github.com/supabase/wrappers/blob/main/README.md)
```shell
git clone https://github.com/supabase/wrappers.git
cd wrappers
git apply <GUFI>/contrib/fdw/supabase-wrappers.patch
cargo install --locked cargo-pgrx
cargo pgrx init # might need --pg-config
```

### gufi_fdw
```shell
cd wrappers # the wrappers directory inside the wrappers repository
ln -s <GUFI>/contrib/fdw/src src/fdw/gufi_fdw
```

## Build and Run PostgreSQL
```shell
cargo pgrx run --features gufi_fdw
```

## PostgreSQL
See [PostgreSQL Documentation](https://www.postgresql.org/docs/current/postgres-fdw.html#POSTGRES-FDW-EXAMPLES)
```sql
CREATE EXTENSION gufi_fdw;

CREATE SERVER gufi_server
    FOREIGN DATA WRAPPER gufi_fdw
    OPTIONS (gufi_query '<path to gufi_query>', index_root '<path to index>');

CREATE FOREIGN TABLE
    entries (name TEXT, type TEXT, inode TEXT, mode INT, nlink INT,
             uid INT, gid INT, size INT, blksize INT, blocks INT,
             atime INT, mtime INT, ctime INT, linkname TEXT,
             xattr_names BYTEA, crtime INT,
             ossint1 INT, ossint2 INT, ossint3 INT, ossint4 INT,
             osstext1 TEXT, osstext2 TEXT)
    SERVER gufi_server
    OPTIONS (table 'entries');

CREATE FOREIGN TABLE
    summary (name TEXT, type TEXT, inode TEXT, mode INT, nlink INT,
             uid INT, gid INT, size INT, blksize INT, blocks INT,
             atime INT, mtime INT, ctime INT, linkname TEXT,
             xattr_names BYTEA, totfiles INT, totlinks INT,
             minuid INT, maxuid INT, mingid INT, maxgid INT,
             minsize INT, maxsize INT, totzero INT, totltk INT,
             totmtk INT, totltm INT, totmtm INT, totmtg INT,
             totmtt INT, totsize INT, minctime INT, maxctime INT,
             minmtime INT, maxmtime INT, minatime INT, maxatime INT,
             minblocks INT, maxblocks INT, totxattr INT, depth INT,
             mincrtime INT, maxcrtime INT,
             minossint1 INT, maxossint1 INT, totossint1 INT,
             minossint2 INT, maxossint2 INT, totossint2 INT,
             minossint3 INT, maxossint3 INT, totossint3 INT,
             minossint4 INT, maxossint4 INT, totossint4 INT,
             rectype INT, pinode TEXT, isroot INT, rollupscore INT)
    SERVER gufi_server
    OPTIONS (table 'summary');
```

## Query
```sql
SELECT name, type, size FROM entries;
SELECT name, type, size FROM summary;
```

## Notes
- Because PostgreSQL parses the input query, SQLite3 and GUFI
  functions cannot be called directly. Views should be created with
  those function calls
  already performed.
