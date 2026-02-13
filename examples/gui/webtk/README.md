# webtk

This directory contains a combined example web and tk GUFI frontend.

This directory is not copied into the build directory, so run from here.

## Dependencies

- Python 3
    - bottle  (for web interface)
    - tkinter (for tk GUI)

## Setup
Build GUFI and create at least one index to have a path to fill in for the next step.

`webtk.py` uses a database to store information, including index locations. Use `makegenericdb` to generate this database file. However, before generating the database file, the `.sql` files in the `generic_fullapps` directory need to be modified with user-specific information:

- `LOCALSEARCHPATH` - path to a local index
- `LOCALSOURCEPATH` - path to the source tree of `LOCALSEARCHPATH`
- `REMOTESEARCHPATH` - path to an index on a remote
- `REMOTESOURCEPATH` - path to the soure tree of `REMOTESEARCHPATH`

Then run

```bash
./makegenericdb
```

to generate the database file `gufigeneric.db`.

## Run

### Web Interface
```bash
webtk.py gufigeneric.db gufi_vt.so web
```

Then in a browser, go to `localhost:8080`

### tkinter GUI
```bash
webtk.py gufigeneric.db gufi_vt.so tk
```