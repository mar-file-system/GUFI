# External Data to External Databases Workflow

Data from external sources may be in any format. In order to get
external data into a format usable by GUFI, these scripts implement a
generic workflow (that uses some caller provided context) that
converts arbitrary data into external databases.

This workflow copies the original external data into a spread tree
first. The spread tree shards are then copied to external databases of
a GUFI tree with a common prefix. The external database can then be
queried using `gufi_query --external-copy`. The permissions of the
external databases will be set so that only those with read access to
those files may read them.

## External Data
External data is generated from the corresponding source filesystem
that an index was generated from. This data may come in many forms,
such as a single file containing all external data, multiple files
with the external data spread across them, or an entire tree of
files. The file or files may or may not be SQLite 3 databases. The
external data might not even exist by the time this workflow is used,
and is instead generated as it is being inserted into the spread tree.

## One Time Setup

Place the workflow configuration files in a place where they will be reused. Run

```bash
gufi_spread_config.sh path/to/fsid.sql path/to/fsid.db path/to/extdbprefix.sql path/to/extdbprefix.db
```

to set them up.

### `fsid.db`

This database contains a table that tracks filesystems and their
respective indexes. See [`fsid.sql`](fsid.sql) for the schema.

### `extdbprefix.db`

This database contains a table that tracks handling of external
database prefixes: how to create spread tree shards from external
databases, and how to copy data from the shards into external
databases in an index. See [`extdbprefix.sql`](extdbprefix.sql) for
the schema.

## Set Up External Data Specific Context

Using the SQLite 3 CLI, fill in `fsid.db` with information about the
filesystem and respective index.

#### Example

```bash
(
    echo "INSERT INTO fs"
    echo "VALUES ('fsid', '/mnt/filesystem', '/search/filesystem', 'xfs', 'example fsid.db entry');"
) | sqlite3 "fsid.db"
```

Next, fill in `extdbprefix.db`. Three files are required from the
caller because they deal with information specific to each set of
external data:

- A program that copies external data to a shard for a single
  filesystem entry.
    - This file must take in 4 arguments:
        - shard path
        - entry path
        - entry inode
        - entry mtime
    - This program may generate extracted data on the fly instead of
      reading from pre-extracted sources.
    - This program should not print anything to `stdout`
    - See [`examples/spread_tree/copy_to_shard.sh`](/examples/spread_tree/copy_to_shard.sh)

- A database SQL file containing the schema of the external
  database. At least one table requires an `inode` (or equivalent)
  column so that the filesystem metadata can join against it. A
  `mtime` column is also recommended. It is recommended that this
  schema match the shard schema to keep things easy.
    - `CREATE TABLE IF NOT EXISTS` is recommended if adding to
      existing shards instead of overwriting them.
    - See [`examples/spread_tree/schema.sql`](/examples/spread_tree/schema.sql)

- A SQL file containing one or more queries to copy data from the
  shard to the index. This was done to allow for caller defined
  transformations of the data (such as selecting subsets of the shard
  to be copied) instead of having the workflow run `SELECT *` on a
  list of table names.
    - See [`examples/spread_tree/shard_to_extdb.sql`](/examples/spread_tree/shard_to_extdb.sql)

- An optional shard path generator may also be provided. If one is
  not, when inserting into `extdbprefix.db`, pass in `NULL` as its
  value.
    - See [`scripts/spread_tree/gufi_spread_shard_path.sh`](gufi_spread_shard_path.sh)

#### Example
```bash
(
    echo "INSERT INTO extdb"
    echo "VALUES ("
    echo "        'prefix', 'examples/spread_tree/schema.sql', NULL,"
    echo "        'examples/spread_tree/copy_to_shard.sh', 'spread_tree',"
    echo "        'examples/spread_tree/shard_to_extdb.sql'"
    echo ");"
) | sqlite3 "extdbprefix.db"
```

## External Data to Spread Tree

To convert the original external data into a spread tree, call

```bash
gufi_spread_tree.sh fsid.db extdbprefix.db fsid prefix
```

This will set up the spread tree and generate individual shards using
the caller provided program.

## Spread Tree to External Databases

Once the spread tree has been set up for the filesystem/index and prefix, call

```bash
gufi_spread_to_extdb.sh fsid.db extdbprefix.db fsid prefix
```

to copy data from the spread tree to external databases in the
index. These external databases will all have the same prefix.

## Querying

To query these external databases, use `gufi_query --external-copy`:
```bash
gufi_query \
    -I "CREATE TABLE external_data(inode, ...);" \
    --external-copy "<prefix>.*" "INSERT INTO external_data SELECT ...;" \
    -E "SELECT ... FROM vrpentries LEFT JOIN external_data ON vrpentries.inode == external_data.inode ...; DELETE FROM external_data;" \
    index
```

`DELETE FROM external_data;` is not strictly necessary. It is used to
unload any previous data loaded into the external_data table, freeing
memory and not adding unnecessary `JOIN`s to `-E`.

## Example

See [`test/regression/spread_tree.expected`](/test/regression/spread_tree.expected) for a full walkthrough of these steps.