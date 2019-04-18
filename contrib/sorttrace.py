#!/usr/bin/env python2

import argparse
import functools
import os
import sys

def comparer(lhs, rhs, delim):
    lparts = lhs.split(delim)
    rparts = rhs.split(delim)

    # compare parent paths first
    lparent = os.path.dirname(lparts[0]) + "/"
    rparent = os.path.dirname(rparts[0]) + "/"

    if lparts < rparts:
        return -1;
    elif lparts > rparts:
        return 1

    # compare types
    ltype = lparts[1]
    rtype = rparts[1]

    # d is lexicographically first, but we want it last
    if ltype < rtype:
        return 1
    elif ltype > rtype:
        return -1

    return 0

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Trace Sorter')
    parser.add_argument('-d', '--delim', dest='delim', default='\x1e', help='Column Delimiter')
    args = parser.parse_args()

    bound_cmp = functools.partial(comparer, delim=args.delim)

    lines = sys.stdin.readlines()
    lines.sort(cmp=bound_cmp)
    print ''.join(lines)
