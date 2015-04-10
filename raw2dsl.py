#!/usr/bin/env python

# Converter from our custom raw provenance output to SPADE DSL format.
# See: https://code.google.com/p/data-provenance/wiki/Pipe

from raw2ttl import *

# from abc import ABCMeta

# import argparse
# import sys
# import fileinput
# import string
# import urllib
# import inspect
# from textwrap import dedent
# from pprint import pprint


#### main ###########################################################
if __name__ == "__main__":
    tag_range = {}

    parser = argparse.ArgumentParser(description='Convert DataTracker raw format to input for the SPADE DSL Reporter.')
    parser.add_argument('-minrange', type=int, default=0, help='the minimum range size to be included in the output')
    parser.add_argument('files', metavar='file', nargs='*', help='specify input files')
    args = parser.parse_args()

    converter = RawTTLConverter(minrange = args.minrange)

    for line in fileinput.input(args.files):
		converter.process_line(line)
