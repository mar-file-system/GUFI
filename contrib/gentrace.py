#!/usr/bin/env python2

import argparse
import sys

rs = chr(0x1e)

def generate_level(out, parent, dir_count, file_count, current_level, max_level):
    global files, dirs

    for d_name in xrange(dir_count):
        out.write(parent + '/' + str(d_name) + '.d' + rs + 'd' + rs + '1' + rs + '16832' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs +'\n')
        dirs += 1

    # generate all files first
    for f_name in xrange(file_count):
        out.write(parent + '/' + str(f_name) + '.f' + rs + 'f' + rs + '1' + rs + '33188' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs + '\n')
        files += 1

    for d_name in xrange(dir_count):
        # generate each directory with its children
        generate_level(out, parent + '/' + str(d_name) + '.d', dir_count, file_count, current_level + 1, max_level)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Trace Generator')
    parser.add_argument('output',      type=argparse.FileType('w+b'), help='output file')
    parser.add_argument('directories', type=int,                      help='number of directories per directory')
    parser.add_argument('files',       type=int,                      help='number of files per directory')
    parser.add_argument('depth',       type=int,                      help='number of levels')
    args = parser.parse_args()

    files = 0
    dirs = 0

    generate_level(args.output, '', args.directories, args.files, 1, args.depth)

    print files, dirs
