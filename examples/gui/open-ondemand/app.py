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

# app.py
from flask import Flask, render_template, request
from lib.gufi_interface import run_gufi_ls, run_gufi_find, run_gufi_stat, gufi_find_help_text

MyApp = Flask('GUFI GUI')

@MyApp.route('/')
def render_home():
  return render_template('views/home.html')

@MyApp.route('/find', defaults={'subpath': ''})
@MyApp.route('/find/<path:subpath>')
def render_find_view(subpath=''):
  kwargs = request.args
  path = subpath or ""
  results = None
  try:
    results = run_gufi_find(path_prefix=path, args=kwargs)
    return render_template('views/find_view.html', help_text=gufi_find_help_text, results=results, values=kwargs)
  except Exception as e:
    # Log and render a friendly error page
    print(f"Find view: failed to run gufi find: {e}")
    return render_template('views/error.html', message='Failed to run find', details=str(e)), 500

  

@MyApp.route('/ls', defaults={'subpath': ''})
@MyApp.route('/ls/<path:subpath>')
def render_ls_view(subpath=''):
  # If the subpath was given in the URL, use it for ls
  path = subpath or ""
  pathComponents = []
  nodes = []

  try: 
    nodes = run_gufi_ls(path)
    pathComponents = path.split('/') if path else []
    info = run_gufi_stat(path)
  except Exception as e:
    print(f"LS view: failed to run gufi_stat: {e}")
    return render_template('views/error.html', message='Failed to retrieve info for path', details=str(e)), 500
  return render_template('views/ls_view.html',  pathComponents=pathComponents, nodes=nodes, info=info)


@MyApp.route('/error')
def render_error_route():
  # Generic error route for manual testing
  msg = request.args.get('msg', 'An error occurred')
  details = request.args.get('details')
  return render_template('views/error.html', message=msg, details=details)


@MyApp.errorhandler(404)
def not_found_error(err):
  return render_template('views/error.html', message='Page not found', details=str(err)), 404


@MyApp.errorhandler(500)
def internal_error(err):
  # If the exception has an original exception, include it
  details = getattr(err, 'description', None) or str(err)
  return render_template('views/error.html', message='Internal server error', details=details), 500

@MyApp.context_processor
def inject_base_url():
    from flask import request
    # request.script_root is the WSGI mount point (empty string if mounted at root)
    return {"BASE_URL": request.script_root or ""}

if __name__ == "__main__":
  MyApp.run(host='0.0.0.0', port=8080)

