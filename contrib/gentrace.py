#!/usr/bin/env python2

import argparse
import sys

def generate_level_r(out, parent, dir_count, file_count, current_level, max_level, rs = chr(0x1e)):
    # generate all files first
    for f_name in xrange(file_count):
        out.write(parent + 'f.' + str(f_name) + rs + 'f' + rs + '1' + rs + '33188' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs + '\n')

    for d_name in xrange(dir_count):
        out.write(parent + 'd.' + str(d_name) + rs + 'd' + rs + '1' + rs + '16832' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs +'\n')

        if current_level < max_level:
            # generate each directory with its children
            generate_level_r(out, parent + 'd.' + str(d_name) + '/', dir_count, file_count, current_level + 1, max_level, rs)

def generate_level(out, root, dir_count, file_count, max_level, rs = chr(0x1e)):
    # start at root
    out.write(root + rs + 'd' + rs + '1' + rs + '16832' + rs + '1' + rs + '1' + rs + '1'  + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + '1' + rs + rs + rs + rs + '1' + rs + '1' + rs +'\n')
    generate_level_r(args.output, root, args.directories, args.files, 1, args.depth, args.separator)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Trace Generator')
    parser.add_argument('output',      type=argparse.FileType('w+b'), help='output file')
    parser.add_argument('directories', type=int,                      help='number of directories per directory')
    parser.add_argument('files',       type=int,                      help='number of files per directory')
    parser.add_argument('depth',       type=int,                      help='number of levels')
    parser.add_argument('--separator', type=str, default=chr(0x1e),   help='record separator')
    args = parser.parse_args()

    generate_level(args.output, '/', args.directories, args.files, args.depth, args.separator)
