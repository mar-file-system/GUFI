#!/usr/bin/env python3
# This file is part of GUFI, which is part of MarFS, which is released
# under the BSD license.
#
#
# Copyright (c) 2017, Los Alamos National Security (LANS), LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
# OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
# ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#
# From Los Alamos National Security, LLC:
# LA-CC-15-039
#
# Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
# Copyright 2017. Los Alamos National Security, LLC. This software was produced
# under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
# Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
# the U.S. Department of Energy. The U.S. Government has rights to use,
# reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
# ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
# ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
# modified to produce derivative works, such modified software should be
# clearly marked, so as not to confuse it with the version available from
# LANL.
#
# THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
# OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
# IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
# OF SUCH DAMAGE.



from fastmcp import FastMCP
import asyncio
import sqlite3
import sys
import subprocess

SCHEMAFILE='./gufi_schemas.txt'
REMOTEHOST='<remote uri>'
MCPTRANSPORT='streamable-http'
MCPSRVHOST='127.0.0.1'
MCPSRVPORT=8000
GUFIVTLIB='path/to/gufi_vt'

mcp = FastMCP(
    name="server"
)

@mcp.tool
def local_file_index_schema(schema: str) -> str:
    """
       sql query on local file information index
    """
    schemafile=SCHEMAFILE
    try:
        with open(schemafile, mode="r") as f:
            content = f.read()
        return content
    except FileNotFoundError:
        return "Schema file not found."

@mcp.tool
def gufi_version(a: str) -> str:
    """gufi_query -- version"""
    result = subprocess.run(["gufi_query", "--version"], capture_output=True, text=True)
    return str(result)

@mcp.tool
def local_file_index(sqlin: str, wherein: str, searchpath: str) -> list[str]:
  """
       sql query on local file information index
  """
  conn=sqlite3.connect(':memory:')
  try:
    conn.enable_load_extension(True)
    cursor = conn.cursor()
    conn.load_extension(GUFIVTLIB)
    conn.enable_load_extension(False)
    sqlline='%s(\'%s\',1,1,99,NULL,1) %s' % (sqlin,searchpath,wherein)
    print(sqlline, file=sys.stderr)
    cursor.execute(sqlline)
    rows = cursor.fetchall()
    for row in rows:
      yield row
    conn.close()
  except sqlite3.Error as e:
    print(f"An SQLite error occurred: {e}",file=sys.stderr)
    conn.close()
    return f"Error executing query: {str(e)}"
  finally:
    conn.close()
    x=1
  return ''

@mcp.tool
def remote_file_index(sqlin: str, wherein: str, searchpath: str) -> list[str]:
  """
       sql query on remote file information index
  """
  conn=sqlite3.connect(':memory:')
  try:
    conn.enable_load_extension(True)
    cursor = conn.cursor()
    conn.load_extension(GUFIVTLIB)
    conn.enable_load_extension(False)
    sqlline='%s(\'%s\',1,0,99,NULL,0,\'ssh\',\'%s\') %s' % (sqlin,searchpath,REMOTEHOST,wherein)
    print(sqlline, file=sys.stderr)
    cursor.execute(sqlline)
    rows = cursor.fetchall()
    for row in rows:
      yield row
    conn.close()
  except sqlite3.Error as e:
    print(f"An SQLite error occurred: {e}",file=sys.stderr)
    conn.close()
    return f"Error executing query: {str(e)}"
  finally:
    conn.close()
    x=1
  return ''

@mcp.tool
def gufi_location(a: str) -> str:
    """gufi_query location"""
    result = subprocess.run(["which", "gufi_query"], capture_output=True, text=True)
    return str(result)

if __name__ == "__main__":
    # Run the server with HTTP transport
    mcp.run(transport=MCPTRANSPORT, host=MCPSRVHOST, port=MCPSRVPORT)
