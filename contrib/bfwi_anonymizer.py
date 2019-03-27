#!/usr/bin/env python

from base64 import urlsafe_b64encode, urlsafe_b64decode
from binascii import hexlify, unhexlify
import argparse
import hashlib
import os
import sys

# wrapper for the built-in hash function to act as a hashlib hash class
# update() and hexdigest() are not provided
class BuiltInHash:
    def __init__(self, string):
        self.hashed = hex(abs(__builtins__.hash(string)))[2:]
        if len(self.hashed) & 1:
            self.hashed = '0' + self.hashed
        self.hashed = unhexlify(self.hashed)

    def digest(self):
        return self.hashed

# Known hashes
Hashes = {
    'BuiltInHash' : BuiltInHash,
    'md5'         : hashlib.md5,
    'sha1'        : hashlib.sha1,
    'sha224'      : hashlib.sha224,
    'sha256'      : hashlib.sha256,
    'sha384'      : hashlib.sha384,
    'sha512'      : hashlib.sha512,
}

def anonymize(string,                                # the data to hash
              sep = os.sep,                          # character that separates paths
              hash = BuiltInHash,                    # an object that has a 'digest' function
              salt = lambda string, extra_args : "", # a function to salt the each piece of the data, if necessary
              args = None):                          # extra arguments to pass into the salt function
    '''
    Splits the input into chunks, if the input contains path separators; otherwise uses the entire input as one chunk
        Empty strings are not hashed, since they are just path separators
    Salts the chunk, if necessary
    Hashes the chunk into a string
    Converts the hash into Base64 to make it printable characters only
    Recombines the chunks with path separators
    '''
    return sep.join(['' if part == '' else urlsafe_b64encode(hash((salt(part, args) if salt else "") + part).digest()) for part in string.split(sep)])

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Column Anonymizer: Read from stdin. Output to stdout.')
    parser.add_argument('hash',    nargs='?',                   default="BuiltInHash", choices=Hashes.keys(), help='Hash function name')
    parser.add_argument('-i', '--in-delim',   dest='in_delim',  default="\x1e",                               help='Input Column Delimiter')
    parser.add_argument('-o', '--out-delim',  dest='out_delim', default="\x1e",                               help='Output Column Delimiter (Do not use space)')
    args = parser.parse_args()

    # read lines from stdin
    for line in sys.stdin:
        while line[-1] == '\n':
            line = line[:-1]
        nl = True
        out = []
        for idx,column in enumerate(line.split(args.in_delim)):
            #path and linkname
            if idx in [0,13]: # path
                anon = anonymize(column, hash=Hashes[args.hash])
                if len(anon) > 490:
                    nl = False
                    break
                else:
                    out += [anon]
            #leave in case we anony ints
            elif idx in []:
                anon = anonymize(column, hash=Hashes[args.hash])
                # convert numeric columns back to numbers
                out += [str(int(hexlify(urlsafe_b64decode(anon)), 16))]
            else:
                out += [column]
        if nl:
            print args.out_delim.join(out)# + args.out_delim
