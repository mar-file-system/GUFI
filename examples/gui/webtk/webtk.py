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



# which are needed
import sys
import os
import re
import sqlite3
from tkinter import *
import tkinter as tk
from tkinter.ttk import *
from tkinter import ttk
from time import strftime
import time
from tkinter import messagebox
from dataclasses import dataclass
from tkinter import font
from datetime import datetime
import csv
import io
from bottle import route, run, template, request, post, get, response

global global_intoplist
global_intoplist=['>','<','=']
global global_logoplist
global_logoplist=['lor','land']
global global_textoplist
global_textoplist=['like','=']
global global_ogoplist
global_ogoplist=['asc','desc']

@dataclass
class gufi_partreq:
  name: int
  num: str
  req: str

@dataclass
class gufi_formin:
  varcnt : int
  op: str
  val: str
  parttype: str
  valtype: str
  joinnm: str
  selectnm: str
  interimnm: str
  displaynm: str
  partnum: int
  shortnm: str
  preselect: str
  postselect: str
  prewhere: str
  postwhere: str

@dataclass
class gufi_global:
  rowcount : int
  rowmax: int

@dataclass
class gufi_apps:
  appname: str
  apptitle: str
  apptitletext: str
  appdebug: int
  appselwhereinputrequired: int
  appvt: str
  appthreads: int
  appsearchtree: str
  appsourcetree: str
  appurl: str
  appurlsourcepath: str

@dataclass
class gufi_apppart:
  apppartname: str
  apppartnum: int
  appparttype: str
  apppartheading: str
  apppartheadingitext: str
  apppartquery: str
  appparttablewidth: int
  apppartlevel: int
  apppartrequired: int

@dataclass
class gufi_appforminfo:
  appfname: str
  appfpartnum: int
  appftype: str
  appfjoin: str
  appfselectnm: str
  appfinterimnm: str
  appfdisplaynm: str
  appfshortnm: str
  appflongnm: str
  appforder: int
  appfdiff: int
  appfitype: int
  appfv0: str
  appfv1: str
  appfv2: str
  appfv3: str
  appfv4: str
  appfv5: str
  preselect: str
  postselect: str
  prewhere: str
  postwhere: str
  appfparttype: str

@dataclass
class gufi_appformd:
  appdname: str
  appdpartnum: int
  appdinterimnm: str
  appdshortnm: str
  appdorder: int
  appddiff: int
  appdtp: str

@dataclass
class gufi_appformdtree:
  appdtname: str
  appdtpartnum: int
  appdtparttreeitem: str
  appdtpartparm: str

def run_download(all_entries,rq_download):
  file_path = '/tmp/gqb_output.%s.csv' % time.time()
  with open(file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    # Write all rows at once
    writer.writerows(all_entries)
  print(f"Data successfully exported to {file_path}")

def adjust_column_widths(treeview):
    # Dictionary to store max widths for each column
    max_widths = {}

    # Get the font used by the Treeview for accurate measuring
    # We use a default font if a specific style isn't configured
    #font = tkFont.nametofont("TkDefaultFont")

    # Measure heading widths
    for col in treeview["columns"]:
        heading_text = treeview.heading(col)["text"]
        width = len(heading_text)
        #print(width)
        max_widths[col] = width

    # Measure content widths
    for item in treeview.get_children():
        values = treeview.item(item)["values"]
        # Include the special '#0' column text if it's visible
        if '#0' not in treeview["columns"] and treeview.cget("show") in ("tree headings", "tree"):
             # Need to measure the main tree column text if it's shown
             item_text = treeview.item(item)["text"]
             width = len(item_text)
             #print(width)
             if width > max_widths.get('#0', 0):
                 max_widths['#0'] = width

        for i, val in enumerate(values):
            col = treeview["columns"][i]
            width = len(str(val))
            #print(width)
            if width > max_widths.get(col, 0):
                max_widths[col] = width

    # Set the column widths (add some padding, e.g., 10 pixels)
    padding = 2
    awidth=width*8
    for col, width in max_widths.items():
        # The minwidth default is 20, we can set the width explicitly
        treeview.column(col, width=awidth + padding, minwidth=width + padding, stretch=True)

# get all children in tk window
def get_all_children(widget):
    """
    Recursively returns a list of all children of a given widget.
    """
    children = widget.winfo_children()
    for child in children:
        if child.winfo_children():
            children.extend(child.winfo_children())
    return children
# get val
def get_val(cursor,gqapps,parttype):
  retval=''
  #qrformin_qq='select val from  qrformintable where val!=\'\' and parttype=\'v\' limit 1'
  qrformin_qq='select val from  qrformintable where val!=\'\' and parttype=\'%s\' limit 1' % (parttype)
  cursor.execute(qrformin_qq)
  matchcountrun  = cursor.fetchall()
  for row in matchcountrun:
    retval='%s' % row[0]
    #print(row[0])
  return(retval)

# get valconcat
def get_valconcat(cursor,gqapps,parttype,column,withopval,asonly,iswhere,checkselected):
  retval=''
  # get the right concatenating delimiter
  #print("get_valconcat")
  if iswhere>0:
    delim=' and '
  else:
    delim=','

  #qrformin_qq='select val,op,%s,valtype from  qrformintable where val!=\'\' and parttype=\'%s\'' % (column,parttype)
  # added the ability to test to see if an input has been selected
  qrformin_qq='with tmpselected as (select joinnm as tmpjoinnm from qrformintable where val!=\'\' and val!=\'None\' and parttype=\'s\') select val,op,%s,valtype from  qrformintable where val!=\'\' and val!=\'None\' and parttype=\'%s\' and (select count(*) from tmpselected where substr(tmpjoinnm,1,length(tmpjoinnm))=substr(joinnm,1,length(tmpjoinnm)))>=%s' % (column,parttype,checkselected)
  #qrformin_qq='with tmpselected as (select joinnm as tmpjoinnm from qrformintable where val!=\'\' and parttype=\'s\') select val,op,%s,valtype from  qrformintable where val!=\'\' and parttype=\'%s\' and (select count(*) from tmpselected where substr(tmpjoinnm,1,length(tmpjoinnm))=substr(joinnm,1,length(tmpjoinnm)))>=%s' % (column,parttype,checkselected)
  #print(qrformin_qq)
  cursor.execute(qrformin_qq)
  matchcountrun  = cursor.fetchall()
  for row in matchcountrun:
    outline='qrformintable %s %s %s %s' % (row[0],row[1],row[2],row[3])
    #print(outline)
    #if row[0]=='None':
    #  print('found None')

    # get the right column type from valtype
    if row[3]=='t':
      coltype='TEXT'
    else:
      coltype='INT64'
    columns=row[2].split(' ')
    numcolumns=len(columns)
    if asonly==0:
      colnm=row[2]
    #else:
    if asonly==1:
      if numcolumns==3:
        colnm=columns[0]
      else:
        colnm=row[2]
    if asonly==2:
      if numcolumns==3:
        colnm=columns[2]
      else:
        colnm=row[2]

    # fix the date value and the mode op
    #print('********in get_valconcat string in *******')
    #print(row)
    theval=row[0]
    theop=row[1]
    if parttype=='i':
      if withopval==2:
        if row[3]=='d':
          datecorrect=row[0][:10]
          #qrdt=datetime.strptime(row[0], '%Y-%m-%d')
          qrdt=datetime.strptime(datecorrect, '%Y-%m-%d')
          theval=int(qrdt.timestamp())
        if row[3]=='m':
          if row[1]=='lor':
            theop='|'
          else:
            theop='&'
          modevalue = int(str(row[0]), 8)
          theval='4095 in (%s)' % modevalue
        if row[3]=='t':
          theval='\'%s\'' % row[0]

    # this is fullcolumn op value
    if withopval==2:
      retval+='%s %s %s%s' % (colnm,theop,theval,delim)
    # this is fullcolumn type
    if withopval==1:
      retval+='%s %s%s' % (colnm,coltype,delim)
    # this is fullcolumn
    if withopval==0:
      retval+='%s%s' % (colnm,delim)
  if retval.endswith(','):
    return(retval[:-1])
  if retval.endswith(' and '):
    return(retval[:-5])
  return(retval)

# get valorder
def get_valorder(cursor,gqapps,parttype,queryin_window):
  retval=''
  qrformin_qq='select val,op from  qrformintable where val!=\'\' and parttype=\'%s\'' % (parttype)
  cursor.execute(qrformin_qq)
  matchcountrun  = cursor.fetchall()
  for row in matchcountrun:
    # i think this should be interimnm because that carries through to aggregate and vtmp
    #qrformin_qqlookup='select selectnm from  qrformintable where val!=\'\' and val!=\'None\' and parttype=\'s\' and joinnm=\'%s\'' % (row[0].split('.')[1])
    qrformin_qqlookup='select interimnm from  qrformintable where val!=\'\' and val!=\'None\' and parttype=\'s\' and joinnm=\'%s\'' % (row[0].split('.')[1])
    print(qrformin_qqlookup)
    cursor.execute(qrformin_qqlookup)
    matchcountrunlu  = cursor.fetchall()
    for rowlu in matchcountrunlu:
      print("get_valorder")
      #print(rowlu[0].split(' ')[2])
      print(row[0])
      print(rowlu[0])
      if ' ' in rowlu[0]:
        retval+='%s %s,' % (rowlu[0].split(' ')[2],row[1])
      else:
        retval+='%s %s,' % (rowlu[0],row[1])
    #print(row[0])
    if len(retval)<1:
      print("get_valorder required row not present")
      if queryin_window=='':
        outerr='Order By requested on %s but %s not selected' % (row[0].split('.')[1],row[0].split('.')[1])
        weberr=output_errors_web(outerr)
        return(weberr,'')
      else:
        root.bell()
        messagebox.showerror("Error", "orderby variable not also selected")
  if retval.endswith(','):
    return('',retval[:-1])
  else:
    return('',retval)

# get valgroup
# this provides both groupbyonly and groupbyselected we want to make sure if you select groupby there is a matching
# select record for safety
def get_valgroup(cursor,gqapps,parttype,queryin_window):
  retval=''
  qrformin_qq='select val,op from  qrformintable where val!=\'\' and parttype=\'%s\'' % (parttype)
  cursor.execute(qrformin_qq)
  matchcountrun  = cursor.fetchall()
  for row in matchcountrun:
    # i think this should be interimnm because that carries through to aggregate and vtmp
    #qrformin_qqlookup='select selectnm from  qrformintable where val!=\'\' and val!=\'None\' and parttype=\'s\' and joinnm=\'%s\'' % (row[0].split('.')[1])
    qrformin_qqlookup='select interimnm from  qrformintable where val!=\'\' and val!=\'None\' and parttype=\'s\' and joinnm=\'%s\'' % (row[0].split('.')[1])
    #print(qrformin_qqlookup)
    cursor.execute(qrformin_qqlookup)
    matchcountrunlu  = cursor.fetchall()
    for rowlu in matchcountrunlu:
      #print("get_valgroup")
      #print(rowlu[0].split(' ')[2])
      if ' ' in rowlu[0]:
        retval+='%s,' % (rowlu[0].split(' ')[2])
      else:
        retval+='%s,' % (rowlu[0])
    #print(row[0])
    if len(retval)<1:
      print("get_valroup required row not present")
      if queryin_window=='':
        outerr='Group By requested on %s but %s not selected' % (row[0].split('.')[1],row[0].split('.')[1])
        weberr=output_errors_web(outerr)
        return(weberr,'')
      else:
        root.bell()
        messagebox.showerror("Error", "groupby variable not also selected")
  if retval.endswith(','):
    return('',retval[:-1])
  else:
    return('',retval)

# this is not used anymore i think groupbyseleced and groupbyonly are the same for safety
# we want groupby to be the interimnm of the select record that has same joinnm as the iterimnm in groupby record
# get valgroupbyonly
def get_valgroupbyonly(cursor,gqapps,parttype):
  retval=''
  qrformin_qq='select val,op from  qrformintable where val!=\'\' and parttype=\'%s\'' % (parttype)
  cursor.execute(qrformin_qq)
  matchcountrun  = cursor.fetchall()
  for row in matchcountrun:
    retval+='%s,' % (row[0].split('.')[1])
  if retval.endswith(','):
    return(retval[:-1])
  else:
    return(retval)

def postsubmit_validate(cursor,gqapps,queryin_window):
  # process all the parts that have required input
  #print("postsubmit_validate")
  qrpart='select apppartname,apppartnum,apppartrequired from apppart where apppartname=\'%s\' and apppartrequired>0 order by apppartnum asc' % (gqapps.appname)
  cursor.execute(qrpart)
  qrpartreq = cursor.fetchall()
  rrow_rows=0
  for row in qrpartreq:
    #print(row)
    rrow_rows+=1

  threwerr=0
  # for all the formin parts that require input, check to see if there is input
  for vrow in qrpartreq:
    outline='postsubmit_validate: validate part %s' % (vrow[1])
    #print(outline)
    #print("postsubmit_validate: all formin rows that have a value and not just zero")
    #qrformin_qq='select count(*) from  qrformintable where val!=\'\' and val!=\'0\' and partnum=%s' % (vrow[1])
    qrformin_qq='select count(*) from  qrformintable where val!=\'\' and partnum=%s' % (vrow[1])
    cursor.execute(qrformin_qq)
    matchcountrun  = cursor.fetchall()
    for row in matchcountrun:
      #print(row[0])
      dumb=1
    matchcount=row[0]
    if matchcount==0:
      print("postsubmit_validate: required row not present")
      if queryin_window=='':
        #outerr=_error('a required input is needed')
        outerr='a required input is needed'
        weberr=output_errors_web(outerr)
        return(weberr)
      else:
        threwerr=1
        root.bell()
        messagebox.showerror("Error", "Required entry or checkbox not provided")

  # for all the formin there just has to be a variable selected parttype=s
  if threwerr==0:
    #qrformin_qq='select count(*) from  qrformintable where val!=\'\' and val!=\'0\' and parttype=\'s\''
    qrformin_qq='select count(*) from  qrformintable where val!=\'\' and parttype=\'s\''
    cursor.execute(qrformin_qq)
    matchcountrun  = cursor.fetchall()
    for row in matchcountrun:
      #print(row[0])
      dumb=1
    matchcount=row[0]
    if matchcount==0:
      print("postsubmit_validate: nothing selected")
      if queryin_window=='':
        outerr=_error('there has to be at least one variable selected to view')
        weberr=output_errors_web(outerr)
        return(weberr)
      else:
        root.bell()
        messagebox.showerror("Error", "Required checkbox for column to see not provided")

  return('')

# input append a row to gqformin
def append_formin(gqformin,varcnt,queryin_window,gqappforminfo):
  #add a row in gqformin
  gqformin.append(gufi_formin('','','','','','','','','','','','','','',''))

  outline='append_formin: queryin_window is empty varcnt %s interimnm %s' % (varcnt,gqappforminfo.appfinterimnm)
  #print(outline)
  #setup all the values for the form to populate directory or through the gqformin[].var.set function
  gqformin[varcnt].varcnt=varcnt
  #finish building out the empty row with all the variables
  gqformin[varcnt].joinnm=gqappforminfo.appfjoin
  gqformin[varcnt].selectnm=gqappforminfo.appfselectnm
  gqformin[varcnt].interimnm=gqappforminfo.appfinterimnm
  gqformin[varcnt].displaynm=gqappforminfo.appfdisplaynm
  gqformin[varcnt].partnum=gqappforminfo.appfpartnum
  gqformin[varcnt].shortnm=gqappforminfo.appfshortnm
  gqformin[varcnt].preselect=gqappforminfo.preselect
  gqformin[varcnt].postselect=gqappforminfo.postselect
  gqformin[varcnt].prewhere=gqappforminfo.prewhere
  gqformin[varcnt].postwhere=gqappforminfo.postwhere
  # gqformin[varcnt].parttype is filled in as the form is built
  # gqformin[varcnt].valtype is filled in as the form is built

  if queryin_window!='':
    outline='append_formin: queryin_window varcnt %s interimnm %s' % (varcnt,gqappforminfo.appfinterimnm)
    #print(outline)
    #setup all the values for the form to populate directory or through the gqformin[].var.set function
    #finish building out the empty row with all the variables`
    gqformin[varcnt].op = tk.StringVar(queryin_window)
    gqformin[varcnt].val = tk.StringVar(queryin_window)

# input validation handler for positive int
def validate_posint(new_value):
  if new_value == "":
    return True  # Allow empty string for deletion
  try:
    int(new_value)
    return True
  except ValueError:
    # Optionally, ring the system bell on invalid input
    root.bell()
    messagebox.showerror("Error", "Input must contain only numbers.")
    return False

# input validation handler for a text field
def validate_text(new_value):
  if new_value == "":
    return True  # Allow empty string for deletion
  pattern = r'^[a-zA-Z0-9./%_]+$'
  if re.fullmatch(pattern,new_value):
    return True
  else:
    # Optionally, ring the system bell on invalid input
    root.bell()
    messagebox.showerror("Error", "Input must contain valid characters")
    return False

# input validation handler for a text pathlist field
def validate_pathlist(new_value):
  if new_value == "":
    return True  # Allow empty string for deletion
  pattern = r'^[a-zA-Z0-9./%_,]+$'
  if re.fullmatch(pattern,new_value):
    return True
  else:
    # Optionally, ring the system bell on invalid input
    root.bell()
    messagebox.showerror("Error", "Input must contain valid characters")
    return False

# input validation handler for a date
def validate_date(new_value):
  if new_value == "":
    return True  # Allow empty string for deletion
  vallen=len(new_value)
  #print(new_value)
  #print(vallen)
  pattern = r"\d{4}-\d{2}-\d{2}"

  # first four must be integers and has to be > 1960 or epoch will be negative
  if vallen<5:
    try:
      yr=int(new_value)
      if vallen==4:
        if yr<1970:
          root.bell()
          messagebox.showerror("Error", "Input must be of form yyyy-mm-dd and > 1969")
          return False
      return True
    except ValueError:
      root.bell()
      messagebox.showerror("Error", "Input must be of form yyyy-mm-dd")
      return False
  # 5th char must be a dash
  if vallen==5:
    if new_value[4]=='-':
       return True
    else:
      root.bell()
      messagebox.showerror("Error", "Input must be of form yyyy-mm-dd")
      return False
  # 8th char must be a dash
  if vallen==8:
    if new_value[7]=='-':
       return True
    else:
      root.bell()
      messagebox.showerror("Error", "Input must be of form yyyy-mm-dd")
      return False
  # cant be > 10 char
  if vallen>10:
    # Optionally, ring the system bell on invalid input
    root.bell()
    messagebox.showerror("Error", "Input must be of form yyyy-mm-dd")
    return False
  # try out to see if its a valid date
  if vallen>9:
    if re.fullmatch(pattern,new_value):
      try:
        dt=datetime.strptime(new_value, '%Y-%m-%d')
        ts=dt.timestamp()
        #print(dt)
        #print(ts)
        return True
      except ValueError:
        # Optionally, ring the system bell on invalid input
        root.bell()
        messagebox.showerror("Error", "Input must be of form yyyy-mm-dd")
        return False
    else:
      # Optionally, ring the system bell on invalid input
      root.bell()
      messagebox.showerror("Error", "Input must be of form yyyy-mm-dd")
      return False
  # nothing bad found
  return True

# pop a help window
def gufiqb_help():
  """Opens a new Toplevel window with help information."""
  help_window = Toplevel(root)
  help_window.title("Gufi Query Help")
  help_window.geometry("300x200") # Set the size of the help window
  help_label = Label(help_window, text="Under the Queries menu bar click on the query you want to run, then fill in the query form and click submit.", wraplength=280)
  help_label.pack(pady=20, padx=10)
  close_button = Button(help_window, text="Close", command=help_window.destroy)
  close_button.pack(pady=10)

#handle results window refresh (doesnt seem to do much)
def run_queryin_refresh(queryin_window,gqglobal,gqapps,cursor):
  #results_window.destroy()
  #queryin_window.destroy()
  queryin_window.state('normal')

#handle results window close
def run_queryin_close(queryin_window,root):
  queryin_window.destroy()
  #root.state('normal')
  #************** this is pretty magical, the main window kept freezing after
  # destroying queryin_window queryin_window which is a direcdt child of root
  # this seems to have fixed it, i think it wakes up teh root window after the destroy
  # maybe?
  root.update()


# pop a results window
def output_results(cursor,vfq,vqtable,fq,rq_vt,rq_rowmax,queryin_window,gqglobal,gqformin,gqapps,deepdebug,rq_verbose,rq_download):

  if rq_vt=='gufi_vt':
    droptmp='drop table if exists temp.vtmp'
    cursor.execute(droptmp)
  else:
    droptmpv='DROP VIEW if exists vtmp'
    cursor.execute(droptmpv)

  #finally we can run the queries
  if rq_vt=="gufi_vt":
    print(vfq)
    cursor.execute(vfq)
  else:
    cursor.execute(vqtable)
    print(fq)

  # this is the final query
  # ok extract the final answer from the virtual table
  cursor.execute(fq)

  # now we do some fancy interrogation of the sql output to format into rows and columns and put headings in
  all_entries = cursor.fetchall()
  nrows = len(all_entries)
  # this is what happens if the query runs ok but no records are retrieved
  outline='run_query: nrows %s' % nrows
  print(outline)
  if nrows>rq_rowmax:
    closetext='CLOSE - retrieved rows %s > max rows %s' % (nrows,rq_rowmax)
    infotext='retrieved rows %s > max rows %s' % (nrows,rq_rowmax)
    #print("no rows retrieved")
  else:
    if nrows<1:
      closetext='CLOSE - No retrieved rows %s' % (nrows)
      infotext='No retrieved rows %s' % (nrows)
      print("CLOSE - no rows retrieved")
    else:
       closetext='CLOSE - retrieved rows %s' % (nrows)
       infotext='retrieved rows %s' % (nrows)

  # get the number of columns and rows and col names from the query
  ncols = len(cursor.description)
  colnames = [description[0] for description in cursor.description]
  coltypes = [description[1] for description in cursor.description]
  colsize = [description[3] for description in cursor.description]
  #print(colnames)
  # types and size are not implemented in sqlite but they are present in the api for compatability
  #print(coltypes)
  #print(colsize)

  htmlout=f''
  # write out the result
  if queryin_window!='':
    tmphtml=output_result_tk(root,infotext,rq_verbose,vfq,vqtable,fq,colnames,deepdebug,rq_rowmax,all_entries,rq_download,rq_vt)
  else:
    tmphtml=output_result_web('',infotext,rq_verbose,vfq,vqtable,fq,colnames,deepdebug,rq_rowmax,all_entries,rq_download,rq_vt)

  htmlout+=tmphtml

  if rq_vt=='gufi_vt':
    droptmp='drop table if exists temp.vtmp'
    cursor.execute(droptmp)
  else:
    droptmpv='DROP VIEW if exists vtmp'
    cursor.execute(droptmpv)

  return(htmlout)

# output errors in html
def output_errors_web(web_error):

  htmlout=f''
  htmlout+='<html>'
  htmlout+='<head><title>gufi query results</title></head>'
  htmlout+='<body>'
  htmlout+='<h1>gufi error message</h1>'
  htmlout+='<h2>%s</h2>' % web_error
  htmlout+='</body>'
  htmlout+='</html>'
  return htmlout

# output results in html
def output_result_web(root,infotext,rq_verbose,vfq,vqtable,fq,colnames,deepdebug,rq_rowmax,all_entries,rq_download,rq_vt):

  htmlout=f''
  htmlout+='<html>'
  htmlout+='<head><title>gufi query results</title></head>'
  htmlout+='<body>'
  htmlout+='<h2>%s</h2>' % infotext

  # we cant run a download here because we dont have the data anymore we would have to do that in the select screen since this is web
  #if rq_download!='':
  #  downloadtext='download csv file'
  #  download_button = Button(results_window, text=downloadtext, command=lambda: run_download(all_entries,rq_download))

  if rq_verbose=='':
    rq_verbose='0'
  trq_verbose=int(rq_verbose)

  if (trq_verbose>0):
    if rq_vt=="gufi_vt":
      htmlout+='<br>'
      htmlout+='<h3>sql produced</h3>'
      htmlout+=vfq
      htmlout+='<br>'
    else:
      htmlout+='<br>'
      htmlout+='<h3>sql produced</h3>'
      htmlout+=vqtable
      htmlout+='<br>'
    htmlout+='<br>'
    htmlout+=fq
    htmlout+='<br>'

  if trq_verbose>1:
    htmlout+='<h3>detailed debug</h3>'
    htmlout+='<br>'
  else:
    htmlout+='<h3>query results</h3>'
    htmlout+='<br>'

  ncols = len(colnames)
  outline='output_result_web: ncols %s<br>' % ncols
  #htmlout+=outline
  qhtmlout=f''
  qhtmlout+='<table>'
  qhtmlout+="<tr>"
  # fill in the query output with headings
  for cn in colnames:
    qhtmlout+="<th><strong>%s</strong></th>" % cn
    #print(qhtmlout)
  qhtmlout+="</tr>"
  rowcounter=0
  for item in all_entries:
    qhtmlout+="<tr>"
    for i in range(ncols):
       qhtmlout+="<td>%s</td>" % item[i]
       #print(item[i])
       #print(item)
    qhtmlout+="</tr>"
    rowcounter+=1
    if rowcounter>int(rq_rowmax):
      print("breaking")
      break
  qhtmlout+='</table>'

  if rowcounter>=int(rq_rowmax):
    qhtmlout+="row max exceeded %s" % rq_rowmax
  else:
    qhtmlout+="total rows %s" % rowcounter

  dhtmlout=f''
  if trq_verbose>1:
    for line in deepdebug.split('\n'):
      dhtmlout+=line.replace(" ","_")
      dhtmlout+='<br>'

  if trq_verbose>1:
    htmlout+=dhtmlout
  else:
    if rq_download!='y':
      htmlout+=qhtmlout

  htmlout+='</body>'
  htmlout+='</html>'

  if rq_download=='y':
    outline='888888888888 running download   %s' % rq_download
    print(outline)
    # Create an in-memory text buffer
    csv_buffer = io.StringIO()
    # Create a CSV writer object, writing to the in-memory buffer
    # newline='' is required when working with the csv module
    writer = csv.writer(csv_buffer)
    # Write all the rows at once
    writer.writerows(all_entries)
    # Get the CSV content as a string
    csv_string = csv_buffer.getvalue()
    # Close the buffer (optional, as it's in memory, but good practice)
    csv_buffer.close()
    filename = "user_data.csv"
    response.content_type = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
    return str(csv_string)

  return(htmlout)

# output results in TK
def output_result_tk(root,infotext,rq_verbose,vfq,vqtable,fq,colnames,deepdebug,rq_rowmax,all_entries,rq_download,rq_vt):
  # make a new window with a close button to put the results
  results_window = Toplevel(root)
  results_window.title("Gufi Query Results")
  results_window.geometry("1200x800")
  #close_button = Button(results_window, text="Close", command=results_window.destroy)
  #close_button.pack(pady=10)

  debugout=ttk.Label(results_window, text=infotext, wraplength=600)
  debugout.pack(pady=1)

  if rq_download!='':
    downloadtext='download csv file'
    download_button = Button(results_window, text=downloadtext, command=lambda: run_download(all_entries,rq_download))
    download_button.pack(pady=1)
  #close_button = Button(results_window, text=closetext, command=results_window.destroy)
  #close_button = Button(results_window, text=closetext, command=output_results_close(results_window,queryin_window))
  #close_button.pack(pady=10)

  if rq_verbose=='':
    rq_verbose='0'

  trq_verbose=int(rq_verbose)

  if (trq_verbose>0):
    if rq_vt=="gufi_vt":
      debugout=ttk.Label(results_window, text=vfq, wraplength=600)
      debugout.pack(pady=1)
    else:
      debugout=ttk.Label(results_window, text=vqtable, wraplength=600)
      debugout.pack(pady=1)
    debugout=ttk.Label(results_window, text=fq, wraplength=600)
    debugout.pack(pady=1)

  # make a frame in the window to scroll
  frame = ttk.Frame(results_window)
  frame.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)

  # Configure the frame's grid to expand with the window
  frame.grid_rowconfigure(0, weight=1)
  frame.grid_columnconfigure(0, weight=1)

  # Create the Treeview widget
  if trq_verbose>1:
    debcol = ("debug")
    tree = ttk.Treeview(frame, columns=debcol, show='headings')
  else:
    tree = ttk.Treeview(frame, columns=colnames, show='headings')
  tree.grid(row=0, column=0, sticky='nsew')

  # Create the vertical scrollbar
  vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
  vsb.grid(row=0, column=1, sticky='ns')

  # Create the horizontal scrollbar
  hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
  hsb.grid(row=1, column=0, sticky='ew')

  # Configure the Treeview to use the scrollbars
  tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

  # Define headings and column properties
  if trq_verbose<1:
    for colname in colnames:
      #tree.column(colname,anchor='center')
      #probably do string compares of colname for known things like time, date, uid, gid, mode, path name, user, group

      tree.heading(colname, text=colname)
  if trq_verbose>1:
      temph='verboseout'
      #tree.heading(temph, text=temph)

  if trq_verbose>1:
    for line in deepdebug.split('\n'):
      tree.insert("", tk.END, values=(line.replace(" ","_")))
  else:
    checkrowmax=0
    for entry in all_entries:
      #print(entry)
      #tree.insert("", tk.END, values=(entry[0], entry[1],entry[2],entry[3],entry[4],entry[5]))
      tree.insert("", tk.END, values=(entry))
      checkrowmax+=1
      if checkrowmax>rq_rowmax:
        break

  if trq_verbose<1:
    adjust_column_widths(tree)

  return('')

def run_query(gqglobal,gqapps,cursor,gqformin,queryin_window):
  #print("run_query:")
  #print("run_query: global info")
  deepdebug=f''
  rq_rowcount=gqglobal.rowcount
  outline='run_query: rq_rowcount = %s' % (rq_rowcount)
  print(outline)
  deepdebug+='%s\n' % outline
  rq_rowmax=gqglobal.rowmax
  outline='run_query: rq_rowmax = %s' % (rq_rowmax)
  deepdebug+='%s\n' % outline
  print(outline)

  #print("run_query: app info")
  outline='run_query: name %s title %s debug %s swinputrequired %s vt %s threads %s searchtree %s sourcetree %s url %s urlsourcepath %s' % (gqapps.appname,gqapps.apptitle,gqapps.appdebug,gqapps.appselwhereinputrequired,gqapps.appvt,gqapps.appthreads,gqapps.appsearchtree,gqapps.appsourcetree,gqapps.appurl,gqapps.appurlsourcepath)
  #print(outline)
  rq_appname=gqapps.appname
  outline='run_query: rq_appname = %s' % (rq_appname)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_apptitle=gqapps.apptitle
  outline='run_query: rq_apptitle = %s' % (rq_apptitle)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_appdebug=gqapps.appdebug
  outline='run_query: rq_appdebug = %s' % (rq_appdebug)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_vt=gqapps.appvt
  outline='run_query: rq_vt = %s' % (rq_vt)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_threads=gqapps.appthreads
  outline='run_query: rq_threads = %s' % (rq_threads)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_searchtree=gqapps.appsearchtree
  outline='run_query: rq_searchtree = %s' % (rq_searchtree)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_sourcetree=gqapps.appsourcetree
  outline='run_query: rq_sourcetree = %s' % (rq_sourcetree)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_url=gqapps.appurl
  outline='run_query: rq_appurl = %s' % (rq_url)
  deepdebug+='%s\n' % outline
  print(outline)
  rq_urlsourcepath=gqapps.appurlsourcepath
  outline='run_query: rq_urlsourcepath = %s' % (rq_urlsourcepath)
  deepdebug+='%s\n' % outline
  print(outline)

  #print("run_query: formin")
  totforminrows=len(gqformin)

  qrformin_q='create temp table qrformintable (varcnt int,op text,val text,parttype text,valtype text,joinnm text,selectnm text,interimnm text,displaynm text,partnum int64,shortnm text,preselect text,postselect text,prewhere text,postwhere text)'
  cursor.execute(qrformin_q)

  outline='run_query: totforminrows %s' % totforminrows
  print(outline)


  #all_variables = {}
  #for key, value in request.forms.items():
    #all_variables[key] = value
    ##return f"Submitted variables: {all_variables}"
    #print(value)
  if queryin_window=='':
    variable_names=list(request.forms.keys())
    #variable_value=request.forms.get(variable_names[0])
    variable_len=len(variable_names)
    i=0
    while i<variable_len:
      variable_value=request.forms.get(variable_names[i])
      outline='vn %s vv %s' % (variable_names[i],variable_value)
      print(outline)
      i+=1

  grformin=[]
  for i in range(totforminrows):
    grformin.append(gufi_formin('','','','','','','','','','','','','','',''))
    grformin[i].varcnt=gqformin[i].varcnt

    if queryin_window=='':
      opvar='op_%s' % i
      valvar='val_%s' % i
      grformin[i].op = request.forms.get(opvar)
      grformin[i].val = request.forms.get(valvar)
    else:
      grformin[i].op=gqformin[i].op.get()
      grformin[i].val=gqformin[i].val.get()

    grformin[i].parttype=gqformin[i].parttype
    grformin[i].valtype=gqformin[i].valtype
    grformin[i].joinnm=gqformin[i].joinnm

    #grformin[i].selectnm=gqformin[i].selectnm.get()
    tmpnm=gqformin[i].selectnm
    grformin[i].selectnm=tmpnm.replace("'","''")
    #grformin[i].interimnm=gqformin[i].interimnm.get()
    tmpnm=gqformin[i].interimnm
    grformin[i].interimnm=tmpnm.replace("'","''")
    #grformin[i].displaynm=gqformin[i].displaynm.get()
    tmpnm=gqformin[i].displaynm
    grformin[i].displaynm=tmpnm.replace("'","''")

    grformin[i].partnum=gqformin[i].partnum
    grformin[i].shortnm=gqformin[i].shortnm
    grformin[i].preselect=gqformin[i].preselect
    grformin[i].postselect=gqformin[i].postselect
    grformin[i].prewhere=gqformin[i].prewhere
    grformin[i].postwhere=gqformin[i].postwhere
    outline='run_query: i %s varcnt %s op %s val %s parttype %s valtype %s jnm %s snm %s inm %s dnm %s partnum %s shnm %s pres %s posts %s prew %s postw %s' % (i,grformin[i].varcnt,grformin[i].op,grformin[i].val,grformin[i].parttype,grformin[i].valtype,grformin[i].joinnm,grformin[i].selectnm,grformin[i].interimnm,grformin[i].displaynm,grformin[i].partnum,grformin[i].shortnm,grformin[i].preselect,grformin[i].postselect,grformin[i].prewhere,grformin[i].postwhere)
    #print(outline)
    qrformin_ql='insert into qrformintable values (%s,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\' )' % (grformin[i].varcnt,grformin[i].op,grformin[i].val,grformin[i].parttype,grformin[i].valtype,grformin[i].joinnm,grformin[i].selectnm,grformin[i].interimnm,grformin[i].displaynm,grformin[i].partnum,grformin[i].shortnm,grformin[i].preselect,grformin[i].postselect,grformin[i].prewhere,grformin[i].postwhere)

    #what if there is a quote used in the variables like xxx||'/'||
    # this is an attempt and correcting that so sqlite can have 2 single quotes instead of 1
    #qrformin_ql_quote1=qrformin_ql.replace("|\'","|\\'")
    #qrformin_ql_quote2=qrformin_ql_quote1.replace("\'|","\\'|")
    qrformin_ql_quote1=qrformin_ql.replace("|'","|''")
    qrformin_ql_quote2=qrformin_ql_quote1.replace("'|","''|")
    outline='run_query: qrformin_ql %s qrformin_ql_quote1 %s qrformin_ql_quote2 %s' % (qrformin_ql,qrformin_ql_quote1,qrformin_ql_quote2)
    outline='run_query: qrformin_ql %s qrformin_ql_quote1 %s qrformin_ql_quote2 %s' % (qrformin_ql,qrformin_ql_quote1,qrformin_ql_quote2)
    #print(outline)
    #cursor.execute(qrformin_ql_quote2)
    cursor.execute(qrformin_ql)

  qrformin_qq='select * from  qrformintable'
  cursor.execute(qrformin_qq)
  rows = cursor.fetchall()
  for row in rows:
    outline='run_query: varcnt %s op %s val %s parttype %s valtype %s jnm %s snm %s inm %s dnm %s partnum %s shnm %s pres %s posts %s prew %s postw %s' % (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14])
    #print(outline)

  print("run_query:")
  print("run_query: all formin rows that have a value")
  qrformin_qq='select * from  qrformintable where val!=\'\''
  cursor.execute(qrformin_qq)
  rows = cursor.fetchall()
  for row in rows:
    outline='run_query: varcnt %s op %s val %s parttype %s valtype %s jnm %s snm %s inm %s dnm %s partnum %s shnm %s pres %s posts %s prew %s postw %s' % (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14])
    #print(outline)
  #print("run_query:")

  weberr=postsubmit_validate(cursor,gqapps,queryin_window)
  # if there is an error and we are in web mode we need to return an html error
  if queryin_window=='':
    if weberr!='':
      print('****** postsubmit_validate error *****')
      return(weberr)

# put this here for reference as so many calls to these routines
#
#def get_valconcat(cursor,gqapps,parttype,column,withopval,asoyynly,iswhere,checkselected):
#parttype  (i,s,o,g,..)
#column  (jointnm,selectnm,interimnm,displaynm,..)
#withopval = 2
#  if parttype I 
#      fix datetime
#      fix mode
#  withopval   =0   column,delim
#  withopval = 1 column, coltype (text/int64),delim
#  withopval = 2 column, op, value, denim
#
#  asonly (0,1,2).   (Colname as colname). (Splitting on space)
#     asonly=0. (Just use the entire column contents
#     asonly=1 split on space, if 3 cols use col 0 (otherwise use entire col contents
#     asonly=2 split on space, if 3 cols use col2 ( otherwise use entire col contents
#
#iswhere (0,1)
#     iswhere=0 Use comma as delim   iswhere=1 use and as denim
#
#checkselected (0,1)
#    checkselected 0 Do not look up the i row in the s rows
#    checkselected 1  look up the i row in the s rows

  #this concatenates (with comma) all the appfselectnm column from the “S” rows from the formv/appforminfo joined table in this case ‘size,mode’ would be the value
  #rq_selectonly=get_selectonly(cursor,gqapps)
  rq_selectonly=get_valconcat(cursor,gqapps,'s','selectnm',0,0,0,0)
  outline='run_query: rq_selectonly = %s' % (rq_selectonly)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates (with comma) all the appfdisplaynm column from the “S” rows from the formv/appforminfjoined table in this case ‘size,modetotxt(mode) as mode’ would be the value
  rq_displayselected=get_valconcat(cursor,gqapps,'s','displaynm',0,0,0,0)
  outline='run_query: rq_displayselected = %s' % (rq_displayselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates the concatenation of (with and) all the appfselectnm (excluding “as xxx”) plus the operator (if int64 < = >) (if text = like) to produce a valid where clause (for input items).  In this case ‘path() like '%shasta%'’ is the value
  rq_whereonly=get_valconcat(cursor,gqapps,'i','selectnm',2,1,1,0)
  outline='run_query: rq_whereonly = %s' % (rq_whereonly)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates the concatenation of (with and) all the appfselectnm (excluding “as xxx”) plus the operator (if int64 < = >) (if text = like) to produce a valid where clause (for selected items).  In this case ‘path() like '%shasta%'’ is the value
  rq_whereselected=get_valconcat(cursor,gqapps,'i','selectnm',2,1,1,1)
  outline='run_query: rq_whereselected = %s' % (rq_whereselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #(as version) this concatenates the concatenation of (with and) all the appfselectnm (excluding “as xxx”) plus the operator (if int64 < = >) (if text = like) to produce a valid where clause (for input items).  In this case ‘path() like '%shasta%'’ is the value
  rq_whereasonly=get_valconcat(cursor,gqapps,'i','selectnm',2,2,1,0)
  outline='run_query: rq_whereasonly = %s' % (rq_whereasonly)
  deepdebug+='%s\n' % outline
  print(outline)

  #(as version_this concatenates the concatenation of (with and) all the appfselectnm (excluding “as xxx”) plus the operator (if int64 < = >) (if text = like) to produce a valid where clause (for selected items).  In this case ‘path() like '%shasta%'’ is the value
  rq_whereasselected=get_valconcat(cursor,gqapps,'i','selectnm',2,2,1,1)
  outline='run_query: rq_whereasselected = %s' % (rq_whereasselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #this  concatenates (with comma) all the interim variable names mentioned by the user actions, so for S and W its appfinterimnm but for O and G it’s the value provided in the pull down selection.  In this case ‘path,size,mode’ is the value
  rq_interimselected=get_valconcat(cursor,gqapps,'s','interimnm',0,2,0,0)
  outline='run_query: rq_interimselected = %s' % (rq_interimselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates the concatenation of interim names for S and W or the values from the pulldown in O and G plus the variable type) this is used to create iterim/aggregate tables (-IJKG).  In this case ‘path text,size int64,mode int64’ is the value
  rq_interimcreateselected=get_valconcat(cursor,gqapps,'s','interimnm',1,2,0,0)
  outline='run_query: rq_interimcreateselected = %s' % (rq_interimcreateselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates the concatenation of (with and) all the appfinterimnm (excluding “as xxx”) plus the operator (if int64 < = >) (if text = like) to produce a valid where clause.  In this case ‘path like '%shasta%'’ is the value
  #rq_interimwhereselected=get_interimwhereselected(cursor,gqapps)
  rq_interimwhereselected=get_valconcat(cursor,gqapps,'i','interimnm',2,2,1,1)
  outline='run_query: rq_interimwhereselected = %s' % (rq_interimwhereselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates (with comma) the concatenation of order by values from the pulldown selection plus the operator (ascending or descending)
  web_error,rq_orderbytmp=get_valorder(cursor,gqapps,'o',queryin_window)
  # if in web mode and we get an error from get_valorder then we need to return html error
  outline='called get_valorder got :%s,%s:' % (web_error,rq_orderbytmp)
  #print(outline)
  if queryin_window=='':
    if web_error!='':
      outline='called get_valorder got :%s,%s: in queryin_window is not empty returning' % (web_error,rq_orderbytmp)
      #print(outline)
      return web_error
  #rq_orderby='orderby %s' % (rq_orderbytmp)
  rq_orderby='%s' % (rq_orderbytmp)
  #rq_orderbyselected='orderby %s' % (rq_orderbytmp)
  rq_orderbyselected='%s' % (rq_orderbytmp)
  outline='run_query: rq_orderby = %s' % (rq_orderby)
  outline='run_query: rq_orderbyselected = %s' % (rq_orderbyselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates (with comma) group by values from the pulldown selection (and converts to selectnm)
  web_error,rq_groupbytmp=get_valgroup(cursor,gqapps,'g',queryin_window)
  #rq_groupby='groupby %s' % (rq_groupbytmp)
  if queryin_window=='':
    if web_error!='':
      outline='called get_valgroup  got :%s,%s: in queryin_window is not empty returning' % (web_error,rq_groupbytmp)
      #print(outline)
      return web_error
  rq_groupby='%s' % (rq_groupbytmp)
  rq_groupbyselected='%s' % (rq_groupbytmp)
  outline='run_query: rq_groupby = %s' % (rq_groupby)
  outline='run_query: rq_groupbyselected = %s' % (rq_groupbyselected)
  deepdebug+='%s\n' % outline
  print(outline)

  #this concatenates (with comma) group by values from the pulldown selection (just the text in the pulldown)
  # i think this is teh same as groupbyselected now
  #rq_groupbytmp=get_valgroupbyonly(cursor,gqapps,'g')
  #rq_groupby='%s' % (rq_groupbytmp)
  #outline='run_query: rq_groupby = %s' % (rq_groupby)
  rq_groupbyonly='%s' % (rq_groupbytmp)
  outline='run_query: rq_groupbyonly= %s' % (rq_groupbyonly)
  deepdebug+='%s\n' % outline
  print(outline)

  #this is a place holder that you supply if you want the builder to insert the “where” for you, if you want to provide your own “where” word you can
  rq_where='where'
  outline='run_query: rq_where = %s' % (rq_where)
  deepdebug+='%s\n' % outline
  print(outline)

  # get limit
  rq_limit=''
  rq_limit=get_val(cursor,gqapps,'l')
  outline='run_query: rq_limit = %s' % rq_limit
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal minlevel
  rq_minlevel=get_val(cursor,gqapps,'e')
  outline='run_query: rq_minlevel = %s' % rq_minlevel
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal maxlevel
  rq_maxlevel=get_val(cursor,gqapps,'f')
  outline='run_query: rq_maxlevel = %s' % rq_maxlevel
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal owneruid
  rq_owneruid=get_val(cursor,gqapps,'j')
  outline='run_query: rq_owneruid = %s' % rq_owneruid
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal ownergid
  rq_ownergid=get_val(cursor,gqapps,'h')
  outline='run_query: rq_ownergid = %s' % rq_ownergid
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal pathlist_file
  rq_pathlist_file=get_val(cursor,gqapps,'m')
  outline='run_query: rq_pathlist_file = %s' % rq_pathlist_file
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal pathlist
  rq_pathlist=get_val(cursor,gqapps,'n')
  outline='run_query: rq_pathlist = %s' % rq_pathlist
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal tree - this is running readdir locally
  rq_tree=get_val(cursor,gqapps,'r')
  outline='run_query: rq_tree = %s' % rq_tree
  deepdebug+='%s\n' % outline
  print(outline)

  # get traversal treelist - this is pulling paths from a list in appformdtree
  rq_treelist=get_val(cursor,gqapps,'u')
  outline='run_query: rq_treelist = %s' % rq_treelist
  deepdebug+='%s\n' % outline
  print(outline)

  # get download output file
  rq_download=get_val(cursor,gqapps,'d')
  outline='run_query: rq_download = %s' % rq_download
  deepdebug+='%s\n' % outline
  print(outline)

  # get verbose
  rq_verbose=get_val(cursor,gqapps,'v')
  outline='run_query: rq_verbose = %s' % rq_verbose
  deepdebug+='%s\n' % outline
  print(outline)

  qrformin_q='drop table qrformintable'
  cursor.execute(qrformin_q)

  # this is the query we build up
  vq=f""

  # drop in traverse/tree info here
  treeinfoset=0
  searchtreelist=0
  searchtree=rq_searchtree
  if rq_tree!='':
    searchtree='%s/%s' % (rq_searchtree,rq_tree)
    treeinfoset=1
  if (treeinfoset<1) & (rq_treelist!=''):
    searchtree='%s/%s' % (rq_searchtree,rq_treelist)
    treeinfoset=1
  if (treeinfoset<1) & (rq_pathlist!=''):
    searchtree='%s' % (rq_pathlist)
    treeinfoset=1
    if ',' in searchtree:
      searchtreelist=1
  if (treeinfoset<1) & (rq_pathlist_file!=''):
    dumb=1
    #ignore pathlist file for now

  if rq_vt=='gufi_vt':
      if searchtreelist>0:
        vq= f"create virtual table temp.vtmp using gufi_vt(threads=%s," % (rq_threads)
        sl=searchtree.split(',')
        sllen=len(sl)
        slcnt=0
        while slcnt<sllen:
          vq+='index=\"%s\",' % (sl[slcnt])
          slcnt+=1
      else:
        vq= f"create virtual table temp.vtmp using gufi_vt(threads=%s,index=\"%s\"," % (rq_threads,searchtree)

  vqtable=''
  if rq_vt!="gufi_vt":
      vqtable= f"create temp view vtmp as select * from %s(\"%s\",%s)" % (rq_vt,searchtree,rq_threads)

  if rq_vt=='gufi_vt':
    if rq_minlevel!='':
      vq += "min_level="
      vq += rq_minlevel
      vq += ","
    if rq_maxlevel!='':
      vq += "max_level="
      vq += rq_maxlevel
      vq += ","
    if rq_owneruid!='':
      vq += "dir_match_uid="
      vq += rq_owneruid
      vq += ","
    if rq_ownergid!='':
      vq += "dir_match_gid="
      vq += rq_ownergid
      vq += ","

  # so we process the appsqueries table matching on app name and build the query passed to
  qs = "select appsqtype,appsquery from appsqueries where appsqname=\'%s\' order by appsqorder;" % rq_appname
  outline='run_query: processqueries: query %s' % qs
  print(outline)
  cursor.execute(qs)
  rows = cursor.fetchall()
  for row in rows:
    qtype=row[0]
    qw=row[1]

    # now we look for all those keywords you put in your query records in the gufi app db for this app
    # and we use all those variables we filled in with all those queries above
    if qw.find("{whereonly}") != -1:
      if rq_whereonly=="":
        qw = qw.replace("{where}","")
        qw = qw.replace("{whereonly}","")
      else:
        qw = qw.replace("{where}","where")
        qw = qw.replace("{whereonly}",rq_whereonly)
    if qw.find("{whereasonly}") != -1:
      if rq_whereasonly=="":
        qw = qw.replace("{where}","")
        qw = qw.replace("{whereasonly}","")
      else:
        qw = qw.replace("{where}","where")
        qw = qw.replace("{whereasonly}",rq_whereasonly)
    if qw.find("{whereasselected}") != -1:
      if rq_whereasselected=="":
        qw = qw.replace("{where}","")
        qw = qw.replace("{whereasselected}","")
      else:
        qw = qw.replace("{where}","where")
        qw = qw.replace("{whereonly}",rq_whereasselected)
    if qw.find("{selectonly}") != -1:
      if rq_selectonly=="":
        print("selectonly asked for but empty")
      else:
        qw = qw.replace("{selectonly}",rq_selectonly)
    if qw.find("{displayselected}") != -1:
      if rq_displayselected=="":
        print("displayselected asked for but empty")
      else:
        qw = qw.replace("{displayselected}",rq_displayselected)
    if qw.find("{interimselected}") != -1:
      if rq_interimselected=="":
        print("interimselected asked for but empty")
      else:
        qw = qw.replace("{interimselected}",rq_interimselected)
    if qw.find("{interimcreateselected}") != -1:
      if rq_interimcreateselected=="":
        print("interimcreateselected asked for but empty")
      else:
        qw = qw.replace("{interimcreateselected}",rq_interimcreateselected)
    if qw.find("{interimwhereselected}") != -1:
      if rq_interimwhereselected=="":
        qw = qw.replace("{where}","")
        qw = qw.replace("{interimwhereselected}","")
      else:
        qw = qw.replace("{where}","where")
        qw = qw.replace("{interimwhereselected}",rq_interimwhereselected)

    if qw.find("{orderbyselected}") != -1:
      if rq_orderbyselected != "":
        ob="order by %s" % rq_orderbyselected
        qw = qw.replace("{orderbyselected}",ob)
      else:
        qw = qw.replace("{orderbyselected}","")

    # just use orderbyselected (this is not exactly correct it requires selected to be enforced
    if qw.find("{orderbyonly}") != -1:
      if rq_orderbyselected != "":
        ob="order by %s" % rq_orderbyselected
        qw = qw.replace("{orderbyonly}",ob)
      else:
        qw = qw.replace("{orderbyonly}","")

    if qw.find("{groupbyselected}") != -1:
      if rq_groupbyselected != "":
        gb="group by %s" % rq_groupbyselected
        qw = qw.replace("{groupbyselected}",gb)
      else:
        qw = qw.replace("{groupbyselected}","")

    if qw.find("{groupbyonly}") != -1:
      if rq_groupbyonly != "":
        gb="group by %s" % rq_groupbyonly
        qw = qw.replace("{groupbyonly}",gb)
      else:
        qw = qw.replace("{groupbyonly}","")

    if qw.find("{limitrec}") != -1:
      if rq_limit != "":
        ql="limit %s" % rq_limit
        qw = qw.replace("{limitrec}",ql)
      else:
        qw = qw.replace("{limitrec}","")

    # if no wheres of any type exist we have to remove the {where}
    if qw.find("{where}") != -1:
      qw = qw.replace("{where}","")

    outline='run_query: processqueries: qtype = %s query = %s' % (qtype,qw)
    print(outline)

    if qtype=="Z":
      fq = qw
    else:
      if qtype=="R" or qtype=="A" or qtype=="V":
        if qtype=="R":
          vq += "remote_cmd=\"%s\"," % (qw)
        if qtype=="A":
          vq += "remote_arg=\"%s\"," % (qw)
        if qtype=="V":
          vq += "verbose=%s," % (qw)
      else:
        vq += "%s=\"%s\"," % (qtype,qw)

  # and this finishes up the query
  vfq = "%s)" % (vq[:-1])

  htmlout=f''
  tmphtml=output_results(cursor,vfq,vqtable,fq,rq_vt,rq_rowmax,queryin_window,gqglobal,gqformin,gqapps,deepdebug,rq_verbose,rq_download)
  htmlout+=tmphtml

  return(htmlout)

#handler for scroll events
def on_frame_configure(canvas):
  canvas.configure(scrollregion=canvas.bbox("all"))

def handle_pulldown(value):
  g=1
  #print(f"Selected option: {value}")

# process appforminfo per variable record to produce input form info for that record for a input part type
def queryin_processvarinput(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin):
  passedin='processvarinput rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  #print("processvarinput type inttype")
  #print(gqappforminfo.appfinterimnm)
  #print(gqappforminfo.appftype)
  #print(gqappforminfo.appfitype)

  localhtml=f''
  localcol=colcnt
  localcolcnt=0
  # treat dates differently
  if gqappforminfo.appftype=='int64':
    if gqappforminfo.appfitype==1:
      templabel='%s (yyyy-mm-dd)' % gqappforminfo.appfshortnm
      if queryin_window=='':
        #localhtml+=templabel
        #localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,templabel)
        localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
      else:
        ttk.Label(queryin_frame[partcnt], text=templabel).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      if queryin_window=='':
        localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
      else:
        ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    if queryin_window=='':
      #localhtml+=gqappforminfo.appfshortnm
      localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
    else:
      ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
  localcolcnt+=1
  localcol+=1
  if queryin_window!='':
    vposintcmd = (root.register(validate_posint), '%P')
    vdatecmd = (root.register(validate_date), '%P')
    vtextcmd = (root.register(validate_text), '%P')
    intop_var = tk.StringVar(queryin_window)

  # append gqformin row and set the parttype as input
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='i'

  # adjust the validation, width, and op type based on appftype and appfitype
  if gqappforminfo.appftype=='text':
    if queryin_window!='':
      gqformin[varcnt].op.set('like')
    else:
      gqformin[varcnt].op='like'

    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].op, *global_textoplist,command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      #localhtml+=global_textoplist[0]
      localhtml += "<select id=\"%s\" name=\"op_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
      for vl in global_textoplist:
        localhtml += "<option value=\"%s\">%s</option>" % (vl,vl)
      localhtml += "</select>"

  if gqappforminfo.appftype=='int64':
    # normal int64 and timestamp
    if gqappforminfo.appfitype<2:
      if queryin_window!='':
        gqformin[varcnt].op.set('>')
      else:
        gqformin[varcnt].op='>'
      if queryin_window!='':
        option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].op, *global_intoplist,command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
      else:
        #localhtml+=global_intoplist[0]
        localhtml += "<select id=\"%s\" name=\"op_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
        for vl in global_intoplist:
          localhtml += "<option value=\"%s\">%s</option>" % (vl,vl)
        localhtml += "</select>"

    # normal mode
    if gqappforminfo.appfitype==2:
      if queryin_window!='':
        gqformin[varcnt].op.set('lor')
      else:
        gqformin[varcnt].op='lor'
      if queryin_window!='':
        option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].op, *global_logoplist,command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
      else:
        #localhtml+=global_logoplist[0]
        localhtml += "<select id=\"%s\" name=\"op_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
        for vl in global_logoplist:
          localhtml += "<option value=\"%s\">%s</option>" % (vl,vl)
        localhtml += "</select>"
  localcolcnt+=1
  localcol+=1

  # build the input entry box based on type and itype
  if gqappforminfo.appftype=='text':
    # normal text
    gqformin[varcnt].valtype='t'
    if queryin_window!='':
      ttk.Entry(queryin_frame[partcnt], width=20, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vtextcmd).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      #localhtml+='text input'
      dbfv='%s %s %s %s %s %s' % (gqappforminfo.appfv0,gqappforminfo.appfv1,gqappforminfo.appfv2,gqappforminfo.appfv3,gqappforminfo.appfv4,gqappforminfo.appfv5)
      localhtml += "<input type=\"text\" id=\"%s\" name=\"val_%s\" %s>" % (gqappforminfo.appfinterimnm,varcnt,dbfv)
  if gqappforminfo.appftype=='int64':
    # normal int64
    if gqappforminfo.appfitype==0:
      gqformin[varcnt].valtype='i'
      if queryin_window!='':
        ttk.Entry(queryin_frame[partcnt], width=20, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=rowmod,column=localcol,sticky=tk.W)
      else:
        #localhtml+='int input'
        dbfv='%s %s %s %s %s %s' % (gqappforminfo.appfv0,gqappforminfo.appfv1,gqappforminfo.appfv2,gqappforminfo.appfv3,gqappforminfo.appfv4,gqappforminfo.appfv5)
        localhtml += "<input type=\"number\" id=\"%s\" name=\"val_%s\" %s>" % (gqappforminfo.appfinterimnm,varcnt,dbfv)
    # normal mode
    if gqappforminfo.appfitype==2:
      gqformin[varcnt].valtype='m'
      if queryin_window!='':
        ttk.Entry(queryin_frame[partcnt], width=20, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=rowmod,column=localcol,sticky=tk.W)
      else:
        #localhtml+='mode input'
        dbfv='%s %s %s %s %s %s' % (gqappforminfo.appfv0,gqappforminfo.appfv1,gqappforminfo.appfv2,gqappforminfo.appfv3,gqappforminfo.appfv4,gqappforminfo.appfv5)
        localhtml += "<input type=\"number\" id=\"%s\" name=\"val_%s\" %s>" % (gqappforminfo.appfinterimnm,varcnt,dbfv)
    # normal date
    if gqappforminfo.appfitype==1:
      gqformin[varcnt].valtype='d'
      if queryin_window!='':
        ttk.Entry(queryin_frame[partcnt], width=10, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vdatecmd).grid(row=rowmod,column=localcol,sticky=tk.W)
      else:
        localhtml+='date input'
        dbfv='%s %s %s %s %s %s' % (gqappforminfo.appfv0,gqappforminfo.appfv1,gqappforminfo.appfv2,gqappforminfo.appfv3,gqappforminfo.appfv4,gqappforminfo.appfv5)
        localhtml += "<input type=\"datetime-local\" id=\"%s\" name=\"val_%s\" %s>" % (gqappforminfo.appfinterimnm,varcnt,dbfv)

  localcolcnt+=1
  localcol+=1
  #e0.grid(row=3, column=1, sticky=(tk.W,tk.E), pady=5, padx=5)
  return(localcolcnt,localhtml)

# process request to download results
def queryin_processdownload(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin):
  passedin='processdownload rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  localhtml=f''

  # add row to gqformin and set this parttype to select
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='d'

  localcol=colcnt
  localcolcnt=0
  if queryin_window!='':
    gqformin[varcnt].val.set(value='')
  else:
    gqformin[varcnt].val=''

  if queryin_window!='':
    check_button = tk.Checkbutton(queryin_frame[partcnt],text=gqappforminfo.appfshortnm,variable=gqformin[varcnt].val).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    #localhtml+='download checkbox'
    dbfv='%s %s %s %s %s %s' % (gqappforminfo.appfv0,gqappforminfo.appfv1,gqappforminfo.appfv2,gqappforminfo.appfv3,gqappforminfo.appfv4,gqappforminfo.appfv5)
    localhtml += "<input type=\"checkbox\" id=\"%s\" name=\"val_%s\" value=\"y\" %s>" % (gqappforminfo.appfinterimnm,varcnt,dbfv)
    # print the label for the field
    localhtml += "<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
  localcolcnt+=1
  localcol+=1
  return(localcolcnt,localhtml)

# process appforminfo per variable record to produce input form info for that record for a checkbox part type
def queryin_processvarcheckbox(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin):
  passedin='processvarcheckbox rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  localhtml=f''

  # add row to gqformin and set this parttype to select
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='s'

  # set the valtype
  if gqappforminfo.appftype=='text':
    # normal text
    gqformin[varcnt].valtype='t'
  if gqappforminfo.appftype=='int64':
    # normal int64
    if gqappforminfo.appfitype==0:
      gqformin[varcnt].valtype='i'
    # normal mode
    if gqappforminfo.appfitype==2:
      gqformin[varcnt].valtype='m'
    # normal date
    if gqappforminfo.appfitype==1:
      gqformin[varcnt].valtype='d'

  localcol=colcnt
  localcolcnt=0

  if "checked" in gqappforminfo.appfv0+gqappforminfo.appfv1+gqappforminfo.appfv2+gqappforminfo.appfv3+gqappforminfo.appfv4+gqappforminfo.appfv5:
    if queryin_window!='':
      gqformin[varcnt].val.set(value=1)
    else:
      gqformin[varcnt].val='1'
  else:
    if queryin_window!='':
      gqformin[varcnt].val.set(value='')
    else:
      gqformin[varcnt].val=''

  if queryin_window!='':
    check_button = tk.Checkbutton(queryin_frame[partcnt],text=gqappforminfo.appfshortnm,variable=gqformin[varcnt].val).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    outline='checkbox %s checked %s' % (gqappforminfo.appfshortnm,gqformin[varcnt].val)
    #localhtml+=outline
    dbfv='%s %s %s %s %s %s' % (gqappforminfo.appfv0,gqappforminfo.appfv1,gqappforminfo.appfv2,gqappforminfo.appfv3,gqappforminfo.appfv4,gqappforminfo.appfv5)
    localhtml += "<input type=\"checkbox\" id=\"%s\" name=\"val_%s\" value=\"y\" %s>" % (gqappforminfo.appfinterimnm,varcnt,dbfv)
    # print the label for the field
    localhtml += "<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
    #localhtml += ' rowcnt %s colcnt %s varcnt %s' % (rowcnt,colcnt,varcnt)

  localcolcnt+=1
  localcol+=1
  return(localcolcnt,localhtml)

# process query limit
def queryin_processlimit(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,groupbycnt):
  passedin='processlimit rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  localhtml=f''

  if queryin_window!='':
    vposintcmd = (root.register(validate_posint), '%P')

  localcol=colcnt
  localcolcnt=0

  # add a row to gqformin and set type to group
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='l'

  # display the group by instance
  if queryin_window!='':
    ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    localhtml+='limit'
  localcolcnt+=1
  localcol+=1

  # display limit entry box
  gqformin[varcnt].valtype='i'
  if queryin_window!='':
    ttk.Entry(queryin_frame[partcnt], width=20, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    #localhtml+='collect limit'
    dbfv='%s %s %s %s %s %s' % (gqappforminfo.appfv0,gqappforminfo.appfv1,gqappforminfo.appfv2,gqappforminfo.appfv3,gqappforminfo.appfv4,gqappforminfo.appfv5)
    localhtml += "<input type=\"number\" id=\"%s\" name=\"val_%s\" %s>" % (gqappforminfo.appfinterimnm,varcnt,dbfv)
  localcolcnt+=1
  localcol+=1
  return(localcolcnt,localhtml)

# process appforminfo per variable record to produce input form info for that record for a groupby part type
def queryin_processvargroupby(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,groupbycnt):
  passedin='processvargroupby rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  localhtml=f''
  localcol=colcnt
  localcolcnt=0

  # add a row to gqformin and set type to group
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='g'

  # display the group by instance
  if queryin_window!='':
    ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    outline='groupby %s' % gqappforminfo.appfshortnm
    #localhtml+=outline
    localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
  localcolcnt+=1
  localcol+=1

  # get the pulldown list for variables for group by from appformd and display the pulldown
  #print("queryin_processvargroupby select")
  grlist=[]
  qbappformd='select appdname,appdpartnum,appdinterimnm,appdshortnm,appdorder,appddiff,appdtp from appformd where appdname=\'%s\' and appdpartnum=%s and appddiff=%s order by appdorder asc' % (gqappforminfo.appfname,gqappforminfo.appfpartnum,groupbycnt)
  #print(qbappformd)
  cursor.execute(qbappformd)
  grows = cursor.fetchall()
  grow_rows=0
  if queryin_window=='':
    localhtml += "<select id=\"%s\" name=\"val_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
    localhtml += "<option value=\"\"></option>"
  for grow in grows:
    #print("queryin_processvargroupby appformd")
    gqappformd=gufi_appformd(grow[0],grow[1],grow[2],grow[3],grow[4],grow[5],grow[6])
    grentry='%s.%s.%s' % (grow[3],grow[2],grow[6])
    grlist.append(grentry)
    #print(gqappformd.appdshortnm)
    if queryin_window=='':
      localhtml += "<option value=\"%s\">%s</option>" % (grentry,grentry)
    grow_rows+=1
  if queryin_window=='':
    localhtml += "</select>"
  if grow_rows==0:
    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].val,'' , command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      localhtml+='no groupby options'
  else:
    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].val, *grlist, command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
  localcolcnt+=1
  localcol+=1

  return(localcolcnt,localhtml)

# process tree for listing sourcepaths
def queryin_processtree(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,gqapps):
  passedin='queryin_processtree rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  localhtml=f''
  localcol=colcnt
  localcolcnt=0

  # add a row to gqformin and set the type to tree
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='r'

  # display the orderby instance
  if queryin_window!='':
    ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    #localhtml+='tree'
    localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
  localcolcnt+=1
  localcol+=1

  # get the pulldown list for variables from appformd and display the pulldown
  #print("queryin_processtree select")
  rrlist=[]
  entries = [d for d in os.listdir(gqapps.appsearchtree) if os.path.isdir(os.path.join(gqapps.appsearchtree,d))]
  if queryin_window=='':
    localhtml += "<select id=\"%s\" name=\"val_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
    localhtml += "<option value=\"\"></option>"
  for entry in entries:
    #print("tree finder")
    #print(entry)
    if queryin_window=='':
      localhtml += "<option value=\"%s\">%s</option>" % (entry,entry)
    dumb=1
  localhtml += "</select>"
  #print("queryin_processtree appformdtree")
  rrlist.append(entry)
  if queryin_window!='':
    option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].val, *entries, command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
  localcolcnt+=1
  localcol+=1

  return(localcolcnt,localhtml)

# process treelist for listing sourcepaths
def queryin_processtreelist(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin):
  passedin='queryin_processtreelist rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  localhtml=f''
  localcol=colcnt
  localcolcnt=0

  # add a row to gqformin and set the type to treelist
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='u'

  # display the orderby instance
  if queryin_window!='':
    ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    #localhtml+='treelist'
    localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
  localcolcnt+=1
  localcol+=1

  # get the pulldown list for variables for order by from appformdtree and display the pulldown
  #print("queryin_processtreelist select")
  trlist=[]
  qbappformdtree='select appdtname,appdtpartnum,appdtparttreeitem,appdtpartparm from appformdtree where appdtname=\'%s\' and appdtpartnum=%s' % (gqappforminfo.appfname,gqappforminfo.appfpartnum)
  #print(qbappformdtree)
  cursor.execute(qbappformdtree)
  trows = cursor.fetchall()
  trow_rows=0
  if queryin_window=='':
    localhtml += "<select id=\"%s\" name=\"val_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
    localhtml += "<option value=\"\"></option>"
  for trow in trows:
    #print("queryin_processtreelist appformdtree")
    gqappformdtree=gufi_appformdtree(trow[0],trow[1],trow[2],trow[3])
    trentry='%s' % (trow[2])
    trlist.append(trentry)
    trow_rows+=1
  if trow_rows==0:
    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].val, '', command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      localhtml+='treelist none'
  else:
    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].val, *trlist, command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      #localhtml+=trlist[0]
      localhtml += "<option value=\"%s\">%s</option>" % (trentry,trentry)
  localhtml += "</select>"
  localcolcnt+=1
  localcol+=1

  return(localcolcnt,localhtml)

# process appforminfo per variable record to produce input form info for that record for a orderby part type
def queryin_processvarorderby(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,orderbycnt):
  passedin='queryin_processvarorderby rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)

  localhtml=f''
  localcol=colcnt
  localcolcnt=0

  # add a row to gqformin and set the type to orderby
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='o'

  # display the orderby instance
  if queryin_window!='':
    ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    outline='orderby %s' % gqappforminfo.appfshortnm
    #localhtml+=outline
    localhtml+="<label for=\"%s\">%s</label>" % (gqappforminfo.appfinterimnm,gqappforminfo.appfshortnm)
  localcolcnt+=1
  localcol+=1

  # get the pulldown list for variables for order by from appformd and display the pulldown
  #print("queryin_processvarorderby select")
  orlist=[]
  qbappformd='select appdname,appdpartnum,appdinterimnm,appdshortnm,appdorder,appddiff,appdtp from appformd where appdname=\'%s\' and appdpartnum=%s and appddiff=%s order by appdorder asc' % (gqappforminfo.appfname,gqappforminfo.appfpartnum,orderbycnt)
  #print(qbappformd)
  cursor.execute(qbappformd)
  grows = cursor.fetchall()
  grow_rows=0
  if queryin_window=='':
    localhtml += "<select id=\"%s\" name=\"val_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
    localhtml += "<option value=\"\"></option>"
  for grow in grows:
    #print("queryin_processvarorderby appformd")
    gqappformd=gufi_appformd(grow[0],grow[1],grow[2],grow[3],grow[4],grow[5],grow[6])
    grentry='%s.%s.%s' % (grow[3],grow[2],grow[6])
    orlist.append(grentry)
    if queryin_window=='':
      localhtml += "<option value=\"%s\">%s</option>" % (grentry,grentry)
    #print(gqappformd.appdshortnm)
    grow_rows+=1
  if queryin_window=='':
    localhtml += "</select>"
  if grow_rows==0:
    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].val, '', command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      localhtml+='orderby none'
  else:
    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].val, *orlist, command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
  localcolcnt+=1
  localcol+=1

  # display op pulldown for orderby asc or desc
  if queryin_window!='':
    gqformin[varcnt].op.set('asc')
  else:
    gqformin[varcnt].op='asc'
  if queryin_window!='':
    option_menu = tk.OptionMenu(queryin_frame[partcnt] , gqformin[varcnt].op, *global_ogoplist,command=handle_pulldown).grid(row=rowmod,column=localcol,sticky=tk.W)
  else:
    #localhtml+=global_ogoplist[0]
    localhtml += "<select id=\"%s\" name=\"op_%s\">" % (gqappforminfo.appfinterimnm,varcnt)
    for vl in global_ogoplist:
      localhtml += "<option value=\"%s\">%s</option>" % (vl,vl)
    localhtml += "</select>"
  localcolcnt+=1
  localcol+=1
  return(localcolcnt,localhtml)

# process appforminfo per variable record to produce input form info for that recordy2y
def queryin_processvar(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,gqapps):
  passedin='processvar rowcnt %s rowmod %s colcnt %s gqappforminfo.appfpartnum %s partcnt %s varcnt %s ' % (rowcnt,rowmod,colcnt,gqappforminfo.appfpartnum,partcnt,varcnt)
  #print(passedin)
  #we count the number of columns we use and return that value
  processed=0
  groupbycnt=0
  orderbycnt=0
  localcol=colcnt
  localcolcnt=0
  localcolincr=0
  localhtml=f''
  localhtmlconcat=f''

  if gqapppart.appparttype=='checkbox':
    #print("found a checkbox")
    #print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processvarcheckbox(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin)
    processed=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml
  if gqapppart.appparttype=='input':
    #print("found an input")
    #print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processvarinput(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin)
    processed=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml
  if gqapppart.appparttype=='groupby':
    #print("found an groupby")
    #print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processvargroupby(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,groupbycnt)
    processed=1
    groupbycnt+=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml
  if gqapppart.appparttype=='orderby':
    #print("found an orderby")
    #print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processvarorderby(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,orderbycnt)
    processed=1
    orderbycnt+=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml
  if gqapppart.appparttype=='limit':
    #print("found an limit")
    #print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processlimit(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,orderbycnt)
    processed=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml
  if gqapppart.appparttype=='downloadf':
    #print("found a downloadf")
    #print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processdownload(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin)
    processed=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml
  if gqapppart.appparttype=='treelist':
    print("found a treelist")
    print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processtreelist(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin)
    processed=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml
  if gqapppart.appparttype=='tree':
    #print("found a tree")
    #print(gqapppart.apppartnum)
    localcolincr,localhtml=queryin_processtree(cursor,rowcnt,rowmod,colcnt,gqapppart,gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,gqapps)
    processed=1
    #print(localcolincr)
    #print(localhtml)
    localhtmlconcat+=localhtml

  localcolcnt+=localcolincr

  if processed == 0:
    if queryin_window!='':
      ttk.Label(queryin_frame[partcnt], text=gqappforminfo.appfshortnm).grid(row=rowmod,column=localcol,sticky=tk.W)
    else:
      localhtml+='fell thru variable types'
    localcolcnt+=1
    localcol+=1
    intoplist=['>','<','=']
    if queryin_window!='':
      intop_var = tk.StringVar(queryin_window)
    if queryin_window!='':
      intop_var.set(intoplist[0])
    else:
      intop_var=intoplist[0]
    if queryin_window!='':
      option_menu = tk.OptionMenu(queryin_frame[partcnt] , intop_var, *intoplist)
      option_menu.grid(row=rowmod, column=localcol, padx=1, pady=1, sticky='w')
    else:
      localhtml+='fell thru variable types pulldown'
    localcolcnt+=1
    localcol+=1
    if queryin_window!='':
      ttk.Label(queryin_frame[partcnt], text="holder").grid(row=rowmod,column=localcol,sticky=tk.W,padx=10)
    else:
      localhtml+='fell thru variable types label'
    localcolcnt+=1
    localcol+=1
    localhtmlconcat+=localhtml

  return(localcolcnt,localhtmlconcat)

# run query input form in html
@post('/')
def run_queryin_web():
  webdb=sys.argv[1]
  gufivt=sys.argv[2]
  outline='run_queryin_web: webdb %s gufivt %s' % (webdb,gufivt)
  #print(outline)

  rqw_appname = request.forms.get('menuinapp')
  rqw_webmode = request.forms.get('webmode')
  #print(rqw_appname)
  #print(rqw_webmode)

  #for key, value in request.forms.items():
  #  print(f"run_queryin_web: formin var name {key},  value {value}")

  #sys.exit()

  # open memory db so if we write we write to memory
  conn = sqlite3.connect(':memory:')
  cursor = conn.cursor()
  # load the gufi virtual table extension into sqlite
  conn.enable_load_extension(True)
  conn.load_extension(gufivt)
  conn.enable_load_extension(False)
  # attach the gufi web app db
  webdburi='file:%s?mode=ro' % webdb
  dbatt='attach database "%s" as webapp;' % webdburi
  cursor.execute(dbatt)

  #get global settings from db do you want rowcount and romax
  qbglobal='select rowcount,rowmax from global;'
  cursor.execute(qbglobal)
  rows = cursor.fetchall()
  for row in rows:
    gqglobal=gufi_global(row[0],row[1])
  #print(gqglobal)
  # get all the information about this app use the apptitle for teh queries pulldown
  qbapp='select appname,apptitle,apptitletext,appdebug,appselwhereinputrequired,appvt,appthreads,appsearchtree,appsourcetree,appurl,appurlsourcepath from apps where appname=\'%s\';' % rqw_appname
  cursor.execute(qbapp)
  rows = cursor.fetchall()
  for row in rows:
    gqapps=gufi_apps(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10])
    #print(gqapps)

  queryinpage=run_queryin(gqglobal,gqapps,cursor,'',rqw_webmode)
  outline='run_queryin_web: returning %s' % queryinpage

  #cursor.execute('detach database webapp;')
  conn.close()

  #print(outline)
  return queryinpage

# this is how we get the query input form generated for tk function
def run_queryin(gqglobal,gqapps,cursor,root,webmode):
  #print(gqapps.appname)
  #print(webmode)

  if webmode!='tk':
    response.content_type = 'text/html'
    debout=f''
    htmlout=f''
    htmlout+='<html>'
    htmlout+='<head><title>gufi query menu</title></head>'
    htmlout+='<body>'
    outline = '<h1>%s</h1>' % gqapps.apptitle
    htmlout+=outline
    htmlout+='<form method="POST" action="/">'
    #htmlout+='<h1>gufi query input</h1>'
    #htmlout+='testing<br>testing<br>'
    #htmlout+='</body>'
    #htmlout+='</html>'
    #return htmlout

  if webmode=='tk':
    # first we make a window separate from the root window and title it with the wep app passed in
    queryin_window = Toplevel(root)
    queryin_window.title(gqapps.apptitle)
    queryin_window.geometry("1000x800") # Set the size of the help window

    # set up some fonts to use
    bold_font = font.Font(family='Helvetica', size=11, weight='bold')
    bigbold_font = font.Font(family='Helvetica', size=14, weight='bold')
    big_font = font.Font(family='Helvetica', size=12)
    reg_font = font.Font(family='Helvetica', size=10)

    # make a container frame inside the queryin_window
    container = ttk.Frame(queryin_window)

    # make a canvas supports scroll commands
    canvas = tk.Canvas(container)
    scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
    scrollbarh = ttk.Scrollbar(container, orient="horizontal", command=canvas.xview)

    # Create the Frame to be Scrolled inside the canvas and we put our widgets into this scrollable_frame
    # This frame will hold all your actual widgets. Its parent is the canvas.
    scrollable_frame = ttk.Frame(canvas)

    # Bind the configure event of the inner frame to update the scroll region see routine on_frame_configure
    scrollable_frame.bind("<Configure>", lambda e: on_frame_configure(canvas))

    # Place the Scrollable Frame within the Canvas
    # create_window puts a widget inside the canvas
    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

    # Configure the Canvas and Scrollbar to work together ---
    canvas.configure(yscrollcommand=scrollbar.set,xscrollcommand=scrollbarh.set)

    # make an array to put each frame we put into the scrollable_frame
    queryin_frame=[]
    #queryin_window.columnconfigure(0, weight=1)
    #queryin_window.rowconfigure(0, weight=1)

  if webmode!='tk':
    queryin_window = ''
    queryin_frame=''
    root=''

  # iterate through all this app parts produce subsubframe for each part heading and each part itself
  partcnt=0
  varcnt=0
  gqformin=[]
  qbapppart='select apppartname,apppartnum,appparttype,apppartheading,apppartheadingitext,apppartquery,appparttablewidth,apppartlevel,apppartrequired from apppart where apppartname=\'%s\' order by apppartnum asc' % (gqapps.appname)
  cursor.execute(qbapppart)
  rows = cursor.fetchall()
  for row in rows:
    gqapppart=gufi_apppart(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8])
    outline='%s %s %s %s %s %s %s %s %s' % (gqapppart.apppartname,gqapppart.apppartnum,gqapppart.appparttype,gqapppart.apppartheading,gqapppart.apppartheadingitext,gqapppart.apppartquery,gqapppart.appparttablewidth,gqapppart.apppartlevel,gqapppart.apppartrequired)
    #print(outline)
    if webmode=='tk':
    #make a frame for the part heading and put in the part heading and part headingtext
      queryin_frame.append('')
      queryin_frame[partcnt] = ttk.Frame(scrollable_frame, padding="4 4 4 4")
      queryin_frame[partcnt].grid(row=partcnt, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
      ttk.Label(queryin_frame[partcnt], text=gqapppart.apppartheading, font=bigbold_font).grid(row=0,column=0,sticky=tk.W)
      ttk.Label(queryin_frame[partcnt], text=gqapppart.apppartheadingitext, font=reg_font).grid(row=1,column=0,sticky=tk.W)
    if webmode!='tk':
      outline='apppart %s apptitle %s partcnt %s<br>' % (gqapppart.apppartheading,gqapppart.apppartheadingitext,partcnt)
      #htmlout+=outline
      htmlout+="<h%s>%s</h%s>" % (gqapppart.apppartlevel,gqapppart.apppartheading,gqapppart.apppartlevel)
      htmlout+="%s<br>" % (gqapppart.apppartheadingitext)
      htmlout+="<br>"

    partcnt+=1

    if webmode=='tk':
      vposintcmd = (root.register(validate_posint), '%P')
      vpathlistcmd = (root.register(validate_pathlist), '%P')
      vtextcmd = (root.register(validate_text), '%P')

      #make a frame for the variables in the part ( from appforminfo table)
      queryin_frame.append('')
      queryin_frame[partcnt] = ttk.Frame(scrollable_frame, padding="1 1 1 1")
      queryin_frame[partcnt].grid(row=partcnt, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    if gqapppart.appparttype=="travcontrol":

      initialsearchtree='Traversal Control: Initial Search Tree: %s' % (gqapps.appsearchtree)
      if webmode=='tk':
        bigbold_font = font.Font(family='Helvetica', size=14, weight='bold')

        #initialsearchtree='Traversal Control: Initial Search Tree: %s' % (gqapps.appsearchtree)
        ttk.Label(queryin_frame[partcnt], text=initialsearchtree, font=bigbold_font).grid(row=0,column=0,columnspan=7,sticky=tk.W,pady=1)

        ttk.Label(queryin_frame[partcnt], text="traversal_min_level:").grid(row=1,column=0,sticky=tk.W,pady=1)
      if webmode!='tk':
        outline='%s<br>' % (initialsearchtree)
        #htmlout+=outline
        outline='traversal_min_level: '
        #htmlout+=outline
        htmlout += "<b>"
        htmlout += "Search tree: "
        htmlout += gqapps.appsearchtree
        htmlout += "</b>"
        htmlout += "<br>"

      gqappforminfo=gufi_appforminfo('','99999','','travcontrol','minlevel','minlevel','minlevel','','','','','','','','','','','','','','','','')
      append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
      gqformin[varcnt].parttype='e'
      if webmode=='tk':
        ttk.Entry(queryin_frame[partcnt], width=2, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=1,column=2,sticky=tk.W)
      if webmode!='tk':
        outline='collect minlevel<br>'
        #print(outline)
        #htmlout+=outline
        htmlout += "<br>"
        htmlout += "<table>"
        htmlout += "<tr>"
        htmlout += "<td>"
        htmlout += "<label for=\"travminlevel\">tree_minlevel</label>"
        outline = '<input type=\"number\" id=\"travminlevel\" name=\"val_%s\" min=\"0\" >' % varcnt
        htmlout += outline
        htmlout += "</td>"

      varcnt+=1

      if webmode=='tk':
        ttk.Label(queryin_frame[partcnt], text="traversal_max_level:").grid(row=1,column=3,sticky=tk.W,pady=1)
      if webmode!='tk':
        outline='traversal_max_level: '
        #print(outline)
        #htmlout+=outline
        htmlout += "<td>"
        htmlout += "<label for=\"travmaxlevel\">tree_maxlevel</label>"

      gqappforminfo=gufi_appforminfo('','99999','','travcontrol','maxlevel','maxlevel','maxlevel','','','','','','','','','','','','','','','','')
      append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
      gqformin[varcnt].parttype='f'
      if webmode=='tk':
        ttk.Entry(queryin_frame[partcnt], width=2, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=1,column=4,sticky=tk.W)
      if webmode!='tk':
        outline='collect maxlevel<br>'
        #print(outline)
        #htmlout+=outline
        outline = '<input type=\"number\" id=\"travmaxlevel\" name=\"val_%s\" min=\"0\" >' % varcnt
        htmlout += outline
        htmlout += "</td>"
      varcnt+=1

      if webmode=='tk':
        ttk.Label(queryin_frame[partcnt], text="traversal_subtreeowner(uid):").grid(row=2,column=0,sticky=tk.W,pady=1)
      if webmode!='tk':
        outline='traversal_subtreeowner(uid): '
        #print(outline)
        #htmlout+=outline
        htmlout += "<td>"
        htmlout += "<label for=\"travuid\">subtreeowner_uid</label>"

      gqappforminfo=gufi_appforminfo('','99999','','travcontrol','owneruid','owneruid','owneruid','','','','','','','','','','','','','','','','')
      append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
      gqformin[varcnt].parttype='j'

      if webmode=='tk':
        ttk.Entry(queryin_frame[partcnt], width=8, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=2,column=2,sticky=tk.W)
      if webmode!='tk':
        outline='collect owneruid<br>'
        #print(outline)
        #htmlout+=outline
        outline = '<input type=\"number\" id=\"travuid\" name=\"val_%s\" min=\"0\" >' % varcnt
        htmlout += outline
        htmlout += "</td>"
      varcnt+=1

      if webmode=='tk':
        ttk.Label(queryin_frame[partcnt], text="traversal_subtree_group_owneri(gid):").grid(row=2,column=3,sticky=tk.W,pady=1)
      gqappforminfo=gufi_appforminfo('','99999','','travcontrol','ownergid','ownergid','ownergid','','','','','','','','','','','','','','','','')
      if webmode!='tk':
        outline='traversal_subtree_group_owner(gid): '
        #print(outline)
        #htmlout+=outline
        htmlout += "<td>"
        htmlout += "<label for=\"travgid\">subtreeowner_gid</label>"
      append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
      gqformin[varcnt].parttype='h'
      if webmode=='tk':
        ttk.Entry(queryin_frame[partcnt], width=8, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=2,column=4,sticky=tk.W)
      if webmode!='tk':
        outline='collect ownergid<br>'
        #htmlout+=outline
        outline = '<input type=\"number\" id=\"travgid\" name=\"val_%s\" min=\"0\" >' % varcnt
        htmlout += outline
        htmlout += "</td>"
        htmlout += "</tr>"
        htmlout += "</table>"
      varcnt+=1

      if webmode=='tk':
        ttk.Label(queryin_frame[partcnt], text="traversal_pathlist_file:").grid(row=3,column=0,sticky=tk.W,pady=1)
      gqappforminfo=gufi_appforminfo('','99999','','travcontrol','pathlist_file','pathlist_file','pathlist_file','','','','','','','','','','','','','','','','')
      if webmode!='tk':
        outline='traversal_pathlist_file: '
        #print(outline)
        #htmlout+=outline
        htmlout += "<br>"
        htmlout += "<label for=\"travpathlistf\">subtree_search_pathlist_file</label>"
      append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
      gqformin[varcnt].parttype='m'

      if webmode=='tk':
        ttk.Entry(queryin_frame[partcnt], width=50, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vpathlistcmd).grid(row=3,column=1,columnspan=5,sticky=tk.W)
      if webmode!='tk':
        outline='collect pathlist_file<br>'
        #htmlout+=outline
        outline = '<input type=\"text\" id=\"travpathlistf\" name=\"val_%s\" size=\"130\">' % varcnt
        htmlout += outline
        htmlout += "<br>"
      varcnt+=1

      if webmode=='tk':
        ttk.Label(queryin_frame[partcnt], text="traversal_pathlist:").grid(row=4,column=0,sticky=tk.W,pady=1)
      if webmode!='tk':
        outline='traversal_pathlist: '
        #print(outline)
        #htmlout+=outline
        htmlout += "<label for=\"travpathlist\">subtree_search_pathlist</label>"
      gqappforminfo=gufi_appforminfo('','99999','','travcontrol','pathlist','pathlist','pathlist','','','','','','','','','','','','','','','','')
      append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
      gqformin[varcnt].parttype='n'

      if webmode=='tk':
        e1=ttk.Entry(queryin_frame[partcnt], width=50, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vpathlistcmd).grid(row=4,column=1,columnspan=5,sticky=tk.W)
      if webmode!='tk':
        outline='collect pathlist<br>'
        #print(outline)
        #htmlout+=outline
        outline = '<input type=\"text\" id=\"travpathlist\" name=\"val_%s\" size=\"130\">' % varcnt
        htmlout += outline
        htmlout += "<br>"

      varcnt+=1
      partcnt+=1

      if webmode=='tk':
        queryin_frame.append('')
        queryin_frame[partcnt] = ttk.Frame(scrollable_frame, padding="1 1 1 1")
        queryin_frame[partcnt].grid(row=partcnt, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

    # for this part do a query to appforminfo table to get all the variables
    # and process them into the rows and columns for this parts frame
    if webmode!='tk':
      htmlout += '<table>'
      htmlout += '<tr>'
    rowcnt=0
    rowmod=0
    #print("tablewidth")
    #print(gqapppart.appparttablewidth)
    colcnt=0
    qbappforminfo='select appfname,appfpartnum,appftype,appfjoin,appfselectnm,appfinterimnm,appfdisplaynm,appfshortnm,appflongnm,appforder,appfdiff,appfitype,appfv0,appfv1,appfv2,appfv3,appfv4,appfv5,preselect,postselect,prewhere,postwhere,appfparttype from appforminfo where appfname=\'%s\' and appfpartnum=\'%s\' order by appfpartnum asc,appforder asc' % (gqapps.appname,gqapppart.apppartnum)
    cursor.execute(qbappforminfo)
    prows = cursor.fetchall()
    for prow in prows:
      gqappforminfo=gufi_appforminfo(prow[0],prow[1],prow[2],prow[3],prow[4],prow[5],prow[6],prow[7],prow[8],prow[9],prow[10],prow[11],prow[12],prow[13],prow[14],prow[15],prow[16],prow[17],prow[18],prow[19],prow[20],prow[21],prow[22])
      outline='%s %s %s %s %s %s %s %s %s %s' % (gqappforminfo.appfname,gqappforminfo.appfpartnum,gqappforminfo.appftype,gqappforminfo.appfjoin,gqappforminfo.appfselectnm,gqappforminfo.appfinterimnm,gqappforminfo.appfdisplaynm,gqappforminfo.appfshortnm,gqappforminfo.appflongnm,varcnt)
      #print(outline)
      # have to use the table width from the apppart table for this part to place the variables
      #if rowcnt % gqapppart.appparttablewidth ==0:
      #if webmode!='tk':
      #  htmlout+='rowcnt %s colcnt %s' % (rowcnt,colcnt)
      if colcnt > gqapppart.appparttablewidth:
        rowmod+=1
        colcnt=0
        rowcnt+=1
        #print('************ resetting row count *********')
        if webmode!='tk':
          htmlout += '</tr>'
          htmlout += '<tr>'
      #print("partnumb, row and col rowmod, width")
      #print(gqappforminfo.appfpartnum,rowcnt,colcnt,rowmod,gqapppart.appparttablewidth)
      # pass all info to this routine to place all the pieces of this variable
      # shortnm, type, etc. into this parts frame, the routine returns number of columns used
      if webmode=='tk':
        colincr,colhtml=queryin_processvar(cursor,rowcnt,rowmod,colcnt,gqapppart, gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,gqapps)
      if webmode!='tk':
        colincr,colhtml=queryin_processvar(cursor,rowcnt,rowmod,colcnt,gqapppart, gqappforminfo,queryin_frame,partcnt,queryin_window,root,varcnt,gqformin,gqapps)
        outline='call query_in_processvar? appforminfo %s %s rowcnt %s colcnt %s varcnt %s<br>' % (gqappforminfo.appfname,gqappforminfo.appfinterimnm,rowcnt,colcnt,varcnt)
        #print(outline)
        htmlout += '<td>'
        htmlout+=colhtml
        htmlout += '</td>'
        #htmlout += ' td colincr %s colcnt %s td ' % (colincr,colcnt)
        #htmlout+=outline
        #print(outline)
        #htmlout+=colhtml
        #colincr=1  #****** this may be wrong we didnt call calincr

      varcnt+=1
      #print("colincr")
      #print(colincr)
      colcnt+=colincr

    if webmode!='tk':
      htmlout += '</tr>'
      htmlout += '</table>'
    partcnt+=1
    #print("partcnt")
    #print(partcnt)

  # add verbose entry
  if webmode=='tk':
    queryin_frame.append('')
    queryin_frame[partcnt] = ttk.Frame(scrollable_frame, padding="10 10 10 10")
    queryin_frame[partcnt].grid(row=partcnt, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
    ttk.Label(queryin_frame[partcnt], text="verbose_level:", font=bold_font).grid(row=3,column=0,sticky=tk.W,pady=1)
  if webmode!='tk':
    #outline='verbose_level: '
    #htmlout+=outline
    htmlout += "<br>"
    htmlout += "<label for=\"verbose\">verbose_level</label>"
  gqappforminfo=gufi_appforminfo('','999999','','verbose_level','verbose','verbose','verbose','','','','','','','','','','','','','','','','')
  append_formin(gqformin,varcnt,queryin_window,gqappforminfo)
  gqformin[varcnt].parttype='v'
  if webmode=='tk':
    ttk.Entry(queryin_frame[partcnt], width=2, validate="key", textvariable=gqformin[varcnt].val, validatecommand=vposintcmd).grid(row=3,column=2,sticky=tk.W)
  if webmode!='tk':
    #print('collect verbose')
    outline='collect verbose<br>'
    #htmlout+=outline
    htmlout+='<input type=\"number\" id=\"verbose\" name=\"val_%s\" min=\"0\" >' % varcnt

  if webmode=='tk':
    # add close button
    #ttk.Button(queryin_frame[partcnt], text="Close", command=queryin_window.destroy).grid(row=4,column=0,sticky=tk.W)
    ttk.Button(queryin_frame[partcnt], text="Close", command=lambda: run_queryin_close(queryin_window,root)).grid(row=4,column=0,sticky=tk.W)

    # add a refresh button
    queryin_refresh = ttk.Button(queryin_frame[partcnt], text="Refresh", command=run_queryin_refresh(queryin_window,gqglobal,gqapps,cursor))
    queryin_refresh.grid(row=5, column=0, columnspan=2, pady=10)

  # add a submit button
  if webmode=='tk':
    queryin_submit = ttk.Button(queryin_frame[partcnt], text="Submit", command=lambda: run_query(gqglobal,gqapps,cursor,gqformin,queryin_window))
    queryin_submit.grid(row=6, column=0, columnspan=2, pady=10)

  if webmode=='tk':
    # Use grid to ensure the canvas expands with the window
    container.pack(fill="both", expand=True)
    canvas.grid(row=0, column=0, sticky="nsew")
    scrollbar.grid(row=0, column=1, sticky="ns")
    scrollbarh.grid(row=1, column=0, sticky="ew")

    # Ensure the container frame expands correctly
    container.grid_rowconfigure(0, weight=1)
    container.grid_columnconfigure(0, weight=1)

    #queryin_window.state('normal')
    #queryin_window.mainloop()

  if webmode!='tk':
    htmlout+='<br>'
    #htmlout+='<form method="POST" action="/">'
    htmlout+='<input type="hidden" id="webmode" name="webmode" value="query">'
    outline='<input type="hidden" id="menuinapp" name="menuinapp" value="%s">' % gqapps.appname
    htmlout+=outline
    htmlout+='<br>'
    htmlout+='<input type="submit" value="Submit">'
    htmlout+='</form>'
    # end testing submit to get run_query to run
    htmlout+='</body>'
    htmlout+='</html>'

    if webmode=='queryin':
      return htmlout
    if webmode=='query':
      outputhtml=run_query(gqglobal,gqapps,cursor,gqformin,'')
      return outputhtml

    return('')

# build menu for queries for web
@route('/')
def build_menu_web():
  webdb=sys.argv[1]
  gufivt=sys.argv[2]
  outline='build_menu_web: webdb %s gufivt %s' % (webdb,gufivt)
  #print(outline)

  # open memory db so if we write we write to memory
  conn = sqlite3.connect(':memory:')
  cursor = conn.cursor()
  # load the gufi virtual table extension into sqlite
  conn.enable_load_extension(True)
  conn.load_extension(gufivt)
  conn.enable_load_extension(False)
  # attach the gufi web app db
  webdburi='file:%s?mode=ro' % webdb
  dbatt='attach database "%s" as webapp;' % webdburi
  cursor.execute(dbatt)

  # write out an html heading stating this is a gufi applictions menu
  response.content_type = 'text/html'
  htmlout=f''
  htmlout+='<html>'
  htmlout+='<head><title>gufi query menu</title></head>'
  htmlout+='<body>'
  htmlout+='<h1>gufi query menu</h1>'
  qs = "select appname,apptitle from apps;"
  cursor.execute(qs)
  rows = cursor.fetchall()

  cursor.execute('detach database webapp;')
  conn.close()

  #print('build_>menu_web:')
  #htmlout+='<form>'
  htmlout+='<form method="POST" action="/">'
  htmlout+='<input type="hidden" id="webmode" name="webmode" value="queryin">'
  htmlout+='<select name="menuinapp" id="menuinapp">'
  for row in rows:
    htmlout+=''
    outline='build_menu_web: %s:  %s' % (row[0],row[1])
    #print(outline)
    outline='<option value="%s">%s</option>' % (row[0],row[1])
    htmlout+=outline
  htmlout+='</select>'
  htmlout+='<br>'
  htmlout+='<input type="submit" value="Submit">'
  htmlout+='</form>'
  htmlout+='</body>'
  htmlout+='</html>'
  #print(htmlout)
  return htmlout

# build menu for queries and help
def build_menu(root,cursor,menubar):
  # Adding File Menu and commands
  queries = Menu(menubar, tearoff = 0)
  menubar.add_cascade(label ='Queries', menu = queries)

  #get global settings from db do you want rowcount and romax
  qbglobal='select rowcount,rowmax from global;'
  cursor.execute(qbglobal)
  rows = cursor.fetchall()
  for row in rows:
    gqglobal=gufi_global(row[0],row[1])

  # get all the information about this app use the apptitle for teh queries pulldown
  qbapp='select appname,apptitle,apptitletext,appdebug,appselwhereinputrequired,appvt,appthreads,appsearchtree,appsourcetree,appurl,appurlsourcepath from apps;'
  cursor.execute(qbapp)
  rows = cursor.fetchall()
  for row in rows:
    gqapps=gufi_apps(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10])
    #print("gqglobal")
    #print(gqglobal.rowcount)
    #print("gqapps")
    #print(gqapps.appname)
    # notice ghe gqapps=gqapps (this is because if you dont do this it puts in the last value in the loop
    queries.add_command(label = gqapps.apptitle, command=lambda gqapps=gqapps: run_queryin(gqglobal,gqapps,cursor,root,'tk'))

  queries.add_separator()
  queries.add_command(label ='Exit', command = root.destroy)

  # Adding Help Menu
  help_ = Menu(menubar, tearoff = 0)
  menubar.add_cascade(label ='Help', menu = help_)
  help_.add_command(label ='Gufi Query Help', command = gufiqb_help)
  help_.add_separator()
  help_.add_command(label ='Gufi Query version 0.0', command = None)

  close_button = Button(root, text="Close All Windows", command=root.destroy)
  close_button.pack(padx=20,pady=20)

# this is used to validate input into this program in the main routine
def validate_input():

#?? for adding web html to the tk path, we would need a parm (tk or web) and if web we would need what is desired
# a menu, an app, or n app results

  # must have 3 args passed in
  #print(len(sys.argv))
  if len(sys.argv) != 4:
    print("syntax gufiqb   path_to_gufi_web_app_db   path_to_gufi_vt_library function (tk,menu,appname.query,appname.result")
    sys.exit(1)
  # test first arg to see if the gufi web app db exists
  print(sys.argv[1])
  if os.path.isfile(sys.argv[1]):
    print("Gufi web app db exists")
  else:
    print("Gufi web app db does not exist")
    sys.exit(1)
  # test second arg to see if the gufi virtual table extension library exists
  #print("argv2")
  print(sys.argv[2])
  if os.path.isfile(sys.argv[2]):
    print("Gufi vt extension library exists")
  else:
    print("Gufi vt extension library  does not exist")
    sys.exit(1)
  if sys.argv[3]=='tk' or sys.argv[3]=='web':
    outline='function is %s' % sys.argv[3]
    print(outline)
  else:
    outline='invalid funcdtion %s must be tk or web' % sys.argv[3]
    print(outline)
    sys.exit(1)


#@post('/bob')
#def run_query_web():
#  print("run_query_web")

#***************main***************

# collect the name of the web app db and the gufi virtual table sqlite extension lib
#validate input
validate_input()
webdb=sys.argv[1]
gufivt=sys.argv[2]
function=sys.argv[3]

# run tk function
if function=='tk':
  # open memory db so if we write we write to memory
  conn = sqlite3.connect(':memory:')
  cursor = conn.cursor()
  # load the gufi virtual table extension into sqlite
  conn.enable_load_extension(True)
  conn.load_extension(gufivt)
  conn.enable_load_extension(False)
  # attach the gufi web app db
  webdburi='file:%s?mode=ro' % webdb
  dbatt='attach database "%s" as webapp;' % webdburi
  cursor.execute(dbatt)

  # creating tkinter window
  root = tk.Tk()
  root.title('Gufi Query')
  #setup menu bar
  # Creating Menubar
  menubar = Menu(root)
  build_menu(root,cursor,menubar)
  # display root window
  root.config(menu = menubar)
  #mainloop()
  root.mainloop()
  sys.exit()

# run web function
if function=='web':
  if __name__ == '__main__':
    # Run the web application
    run(host='localhost', port=8080, debug=True, reloader=True)
    sys.exit()

sys.exit()
