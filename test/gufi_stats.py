#!/usr/bin/env python2

import imp
import os
import sys

file_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(file_dir)
gufi_stats = imp.load_source('gufi_stats', os.path.join(root_dir, 'scripts', 'gufi_stats'))

if __name__=='__main__':
    sys.exit(gufi_stats.run(sys.argv, "config.test"))
