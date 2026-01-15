# webtk

This directory contains a combined example web and tk GUFI frontend.

## Dependencies

- Python 3
    - bottle  (for web interface)
    - tkinter (for tk GUI)

## Setup
Build GUFI and create at least one index to have a path to fill in for the next step.

`webtk.py` uses a database to store information, including index locations. `webtk.sql` contains the SQL to generate this database file. However, before generating the database file, it needs to be modified with user-specific information:

- `<index path>` - path to a local index
- `<remote index path>` - path to an index on a remote
- `<remote>` - the URI of a remote (hostname, IP, etc.)

Then run

```bash
sqlite3 webtk.db < webtk.sql
```

to generate the database file.

## Run

### Web Interface
```bash
webtk.py webtk.db gufi_vt.so web
```

Then in a browser, go to `localhost:8080`

### tkinter GUI
```bash
webtk.py webtk.db gufi_vt.so tk
```