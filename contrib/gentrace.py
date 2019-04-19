#!/usr/bin/env python2

import argparse
import sys

rs = chr(0x1e);

def generate_level(out, parent, dir_count, file_count, current_level, max_level):
    if current_level == max_level:
        return

    for d_name in xrange(dir_count):
        out.write(parent + '/' + str(d_name) + '.d' + rs + 'd' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '\n')

        # generate all files first
        for f_name in xrange(file_count):
            out.write(parent + '/' + str(d_name) + '.d' + '/' + str(f_name) + '.f' + rs + 'f' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '\n')

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

    generate_level(args.output, '', args.directories, args.files, 0, args.depth)
