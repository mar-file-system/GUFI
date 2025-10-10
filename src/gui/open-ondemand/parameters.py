entries_columns = {
    # Columns in the vrpentries view
    "type": {
        "type": "TEXT",
        "description": "character d for file or link"
    },
    "inode": {
        "type": "INT64",
        "description": "inode number from source system"
    },
    "mode": {
        "type": "INT64",
        "description": "unix mode bits"
    },
    "nlink": {
        "type": "INT64",
        "description": "unix number of links"
    },
    "uid": {
        "type": "INT64",
        "description": "unix uid"
    },
    "gid": {
        "type": "INT64",
        "description": "unix gid"
    },
    "size": {
        "type": "INT64",
        "description": "unix logical file size in bytes"
    },
    "blksize": {
        "type": "INT64",
        "description": "unix blksize for file"
    },
    "blocks": {
        "type": "INT64",
        "description": "unix number of blocks"
    },
    "atime": {
        "type": "INT64",
        "description": "unix access time epoch"
    },
    "mtime": {
        "type": "INT64",
        "description": "unix modification time epoch"
    },
    "ctime": {
        "type": "INT64",
        "description": "unix change time epoch"
    },
    "linkname": {
        "type": "TEXT",
        "description": "unix string for link name"
    },
    "xattrs": {
        "type": "TEXT",
        "description": "concatenation of all extended attributes"
    },
    "crtime": {
        "type": "INT64",
        "description": "create time epoch integer (some file systems provide this)"
    },
    "ossint1": {
        "type": "INT64",
        "description": "storage system specific integer"
    },
    "ossint2": {
        "type": "INT64",
        "description": "storage system specific integer"
    },
    "ossint3": {
        "type": "INT64",
        "description": "storage system specific integer"
    },
    "ossint4": {
        "type": "INT64",
        "description": "storage system specific integer"
    },
    "osstext1": {
        "type": "TEXT",
        "description": "storage system specific string"
    },
    "osstext2": {
        "type": "TEXT",
        "description": "storage system specific string"
    }
}

