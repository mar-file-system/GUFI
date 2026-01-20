# MCP Server and Client Example
This is a simple mcp server and client build on FastMCP.
If you wanted to use this in a secure environment you would have to turn on authentication in the mcp server and client.
The demo uses `gufi_vt_pentries` which is a GUFI SQLite Eponymous Virtual Table that is available both locally and remotely
using `gufi_vt`.

## Set Up
```bash
pip install fastmcp
```

You must have sqlite3 installed (e.g. `3.51.1 2025-11-28 17:28:25`).

You must have GUFI built and installed. The `gufi_query` executable must be in your PATH.

```bash
gufi_query --version
```
should be 0.6.10 or later to have `gufi_vt` with remote querying capabilities.

To use these demos please edit the following lines near the top of the following Python files:

`gufi_mcp_client.py`:
```
MCP_SERVER='http://127.0.0.1:8000/mcp'
this is the url for the mcp server you will start

LOCALSELECT='select path,name,size from gufi_vt_pentries'
select part of query

LOCALWHERE='where name like \'%\' limit 50'
where part of query

LOCALSEARCHPATH='local/index/path'
this is the gufi index path

REMOTESELECT='select path,name,size from gufi_vt_pentries'
select part of query

REMOTEWHERE='where name like \'%\' order by size desc limit 50'
where part of query

REMOTESEARCHPATH='remote/index/path'
this is the gufi index path
```

`gufi_mcp_server.py`:
```
SCHEMAFILE='./gufi_schemas.txt'
this is just a text file that can be provided in a an mcp query

REMOTEHOST='<remote uri>'
this is the host where your remote gufi is - gufi_query is run remotely via gufi_vt

MCPTRANSPORT='streamable-http'
this is the mcp transport type

MCPSRVHOST='127.0.0.1'
this is the mcp server host you will be starting

MCPSRVPORT=8000
this is the mcp server port number

GUFIVTLIB='path/to/gufi_vt'
this is the path to the gufi_vt file
```

## Run
To start the server:
```bash
python3 gufi_mcp_server.py
```
It should start up and present the fastmcp server log screen.

To run the client:
```bash
python3 gufi_mcp_client.py
```
Output from the client is provided in mcp.out file in this package.

## MCP Tools
The mcp tools imlemented are

| Tool Name | Description |
| --------- | ----------- |
| `local_file_index_schema` | `gufi_query --version` |
| `gufi_version` | `gufi_query --version` |
| `local_file_index` | SQL query on local file information index |
| `remote_file_index` | SQL query on remote file information index |
| `gufi_location` | `gufi_query` location |

This is just an example but you can see how easy it is to query gufi in an AI MCP compliant agent.
You may want to use json output in your queries and answers as it seems MCP uses json a lot.
