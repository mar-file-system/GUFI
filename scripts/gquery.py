#!/usr/bin/env python3 

import argparse
import shlex
import statistics
import subprocess
import sys
import traceback

from collections.abc import Iterator

import gufi_config as gc

# if user doesn't specify a value for something on the command line, prompt for it
def prompt_for(which, eg, current=None,edit=False):
  print("%s value. %s" % (which.upper(), "Example: %s" % eg if eg and not current else ''))
  if current or edit:
    print("\tCurrent value is '%s'. Press enter to accept or type a new selection or type 'X' to use an empty string." % current)
  clause = input("\t> ")
  if current and len(clause) < 1:
    clause = current  # they accepted the current thing so don't change to empty
  elif clause.lower() == 'x':
    clause = None
  return clause

def get_gufi_command(select, tables, where, nthreads, indexroot):
  query = "select %s from %s %s " % (select,tables,"where %s" % where if where else '')
  command = [
    "gufi_query",
    "-S", query, 
    "-n", str(nthreads),
    indexroot
  ]
  # Convert the command list to a single string for display
  command_str = ' '.join(shlex.quote(arg) for arg in command)
  return (command,command_str)

def execute_command(command,command_string,aggregate_function,Verbose=False):
  if Verbose: print("Will now execute %s." % command_string)
  output = run_command(command)
  if aggregate_function:
    (agg_func_pointer, agg_func_type) = aggregate_functions[aggregate_function][1:3]
    try:
      # small problem. We added distinct which works great for strings but the others need to be converted into ints
      if agg_func_type == int:
        aggregation = agg_func_pointer(map(int,filter(bool,output))) # make sure to convert the output into ints and remove empty strings for the aggregate functions
      elif agg_func_type == str:
        aggregation = agg_func_pointer(output) # make sure to convert the output into ints and remove empty strings for the aggregate functions
      else:
        print(f"Unknown agg func type {agg_func_type}")
        sys.exit(255)
      ret = aggregation
    except Exception as e:
      print(f"Problem with the aggregation function {aggregate_function} which could not convert {output}.\n{e}")
      traceback.print_exc()
      sys.exit(255)
  else:
    ret = output

  return ret

def run_command(command):
  completed_process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout_data = completed_process.stdout
  # Uncomment the following line to debug non-printable characters
  # print(repr(stdout_data))

  output = stdout_data.decode('utf-8').split('\n')
  
  if output[-1] == '':
    output.pop()
  
  if completed_process.returncode != 0:
    print(f"Command failed with error code {completed_process.returncode}")
    print(completed_process.stderr.decode('utf-8'))
  
  return output


def check_args(args,eg_select,eg_tables,eg_where,eg_agg,edit=False):
  if args.confirm:
    args.select = prompt_for('select',eg_select,args.select,edit)
    args.tables = prompt_for('tables',eg_tables,args.tables,edit)
    args.where = prompt_for('where',eg_where,args.where,edit)
    args.aggregate = prompt_for('aggregate',eg_agg,args.aggregate,edit)
  
  (command,command_str) = get_gufi_command(select=args.select, tables=args.tables, where=args.where, nthreads = args.numthreads, indexroot=args.indexroot)

  while args.confirm:
    # Show the command to the user for confirmation
    print(f"Constructed command: {command_str}")
    user_confirm = input("\tProceed? y/e/q (Y to execute now. E to edit any fields. Q to quit.)\n\t> ")

    if user_confirm.lower() == 'y':
      break
    elif user_confirm.lower() == 'q':
      sys.exit(0)
    else:
      if user_confirm.lower() != 'e':
        print("Unknown input. Please try again.")
      return check_args(args,eg_select,eg_tables,eg_where,eg_agg,edit=True)

  # actually exit the function either because confirm was off or because they confirmed
  return (command,command_str)

def count_items(collection):
  if isinstance(collection, Iterator):
    return sum(1 for _ in collection)
  else:
    return len(collection)


def distinct_values(collection):
  ret = set(collection)
  return ret

# the aggregation functions that we currently support
# note that they all must accept a python iterator which is why we can't use len() for count
aggregate_functions = {
 'sum':      ['Print the sum of the values',  sum, int],
 'max':      ['Print the max value',          max, int],
 'min':      ['Print the min value',          min, int],
 'count':    ['Print the number of values',   count_items, int],
 'mean':     ['Print the mean value',         statistics.mean, int],
 'median':   ['Print the median value',       statistics.median, int],
 'mode':     ['Print the mode value',         statistics.mode, int],
 'stdev':    ['Print the standard deviation', statistics.stdev, int],
 'distinct': ['Print distinct values',        distinct_values, str],
}

def detailed_agg_help():
  print("Detailed help for --aggregate:")
  print("You can use the following aggregate functions:")
  for k,v in aggregate_functions.items():
    print("- %s: %s" % (k, v[0]))

def parse_args(gconfig,eg_select,eg_tables,eg_where,eg_agg):
  parser = argparse.ArgumentParser(description="""
      GUFI Simple Query Builder
      Use this tool to build and execute gufi queries by inputting the various pieces of the query individually.
      Some values will pull defaults if they are set in a GUFI config file.
      Values without defaults can be set using command-line args. If not set on command-line, you will be prompted for them.
      """,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('-s', '--select',       help="SQL select statement (e.g. '%s')" % eg_select)
  parser.add_argument('-t', '--tables',       help="SQL tables (e.g. '%s')" % eg_tables)
  parser.add_argument('-w', '--where',        help="SQL where statement (e.g. '%s')" % eg_where)
  parser.add_argument('-n', '--numthreads',   help='Number of threads to use.', default=gconfig.config['Threads'])
  parser.add_argument('-i', '--indexroot',    help='Path to GUFI index to query.', default=gconfig.config['IndexRoot'])
  parser.add_argument('-c', '--confirm',      help='Confirm the query before executing.', action='store_true', default=False)
  parser.add_argument('-m', '--multiple',     help='Run multiple queries instead of simply quitting after the first.', action='store_true', default=False)
  parser.add_argument('-a', '--aggregate',    help='If output is a list of numbers, use an optional aggregation function. Use -H or --helpagg for more info.')
  parser.add_argument('-H', '--helpagg',      help='Provide detailed help for specific options', action='store_true', default=False) 
  parser.add_argument('-v', '--verbose',      help='More verbse output', action='store_true', default=False)
  args = parser.parse_args()

  if args.helpagg:
    detailed_agg_help()
    sys.exit(0) 
  else:
    return args

def query_gconfig():
  # before we parse args, let's use gufi_config to try to pull defaults
  # one weird thing is that we seem to need to hard-code the array of possible config values
  possible_settings = {key: None for key in ['Threads', 'Query', 'Stat', 'IndexRoot', 'OutputBuffer']} 
  gconfig = gc.Config(possible_settings)
  return gconfig

def main(args):
  gconfig = query_gconfig()

  # set some possible example values as variables so we can reuse them
  eg_select = "count(*)"
  eg_tables = "vrpentries f, vrsummary d"
  eg_where = "d.size > 0 and f.size < 2049 and f.pinode = d.inode and level()>2 [or leave blank]"
  eg_agg = "max [or leave blank for no aggregation]"

  args = parse_args(gconfig,eg_select,eg_tables,eg_where,eg_agg)

  while True:
    (command,command_string) = check_args(args,eg_select,eg_tables,eg_where,eg_agg)
    output = execute_command(command,command_string,args.aggregate,args.verbose)
    if args.aggregate:
        if args.verbose:
          print(f"The {args.aggregate} of your query is {output}.")
        else:
          print(output)
    else:
      for line in output:
        print(line)

    if not args.multiple or input("Run another query? y/n\n\t> ").lower() != 'y':
      break

# call main function if executed directly
if __name__ == '__main__':
  main(sys.argv[1:])

