# app.py
from flask import Flask, render_template, request
import sys
import subprocess
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
  MyApp.run(host='0.0.0.0', port=8080, debug=True)

