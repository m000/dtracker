#!/usr/bin/env python

# Converter from our custom raw provenance output to turtle output.
# See: http://www.w3.org/TeamSubmission/turtle/                

import argparse
import sys
import fileinput
import string
import urllib
from textwrap import dedent
from pprint import pprint


#### constants and formats ##########################################
rdf_header = dedent('''
    @prefix prov: <http://www.w3.org/ns/prov#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
''').strip()

rdf_derived_fmt = dedent('''
    <{url_file1}> prov:wasDerivedFrom <{url_file2}> .
''').strip()

rdf_derived_range_fmt = dedent('''
    <{url_file1}{range1}> prov:wasDerivedFrom <{url_file2}{range2}> .
''').strip()

rdf_exec_fmt = dedent('''
    <{url_program}> a prov:Activity . 
''').strip()

rdf_generated_fmt = dedent('''
    <{url_file}> prov:wasGeneratedBy <{url_program}> .
''').strip()

rdf_member_fmt = dedent('''
    <{url_file}> prov:hadMember <{url_file}{range}> .
''').strip()

rdf_open_fmt = dedent('''
    <{url_file}> a prov:Entity .
    <{url_file}> rdfs:label "{label}" .
''').strip()

rdf_used_fmt = dedent('''
    <{url_program}> prov:used <{url_file}> .
''').strip()

file_range_fmt = '#%d-%d'

#### exceptions #####################################################
class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class UnknownUFDError(Error):
    """Raised when there's no mapping for an ufd."""
    def __init__(self, ufd):
        self.ufd = ufd
    def __str__(self):
        return "No active mapping for %s." % (self.ufd)

class TagFormatError(Error):
    """Raised when tags cannot be parsed."""
    def __init__(self, tagspec):
        self.tagspec = tagspec
    def __str__(self):
        return "Cannot parse '%s' into tags." % (self.tagspec)

class RangeError(Error):
    pass


#### data types #####################################################
class Range:
    start = 0
    end = 0

    def __init__(self, start, end=None):
        self.start = start
        self.end = self.start if end == None else end
        if self.end < self.start:
            self.start = self.end
            self.end = start

    def expand(self, n=1):
        self.end += n

    def lexpand(self, n=1):
        self.start -= n

    def length(self):
        return self.start-self.end

    def join(self, range2):
        if not self.is_adjacent(range2):
            raise RangeError("Attempting to join not adjacent ranges.")

    def is_adjacent(self, range2):
        if isinstance(range2, self.__class__):
            if range2.end == self.start-1 or range2.start == self.end+1:
                return True
            return False
        elif isinstance(range2, int):
            if range2 == self.start-1 or range2 == self.end+1:
                return True
            return False
        else:
            raise RangeError("Unsupported argument type.")

    def is_overlapping(self, range2):
        if range2.start<=self.start and range2.end <= self.start:
            return False
        if range2.start>=self.end and range2.end>=self.end:
            return False
        return True

    def __str__(self):
        return "%d-%d" % (self.start, self.end)


#### handlers for entry lines #######################################
def process_c(data):
    global s
    # line format: c:<ufd>
    ufd = data
    filename1 = s.ufdmap[ufd]

    # print triples
    if ufd in s.derived:
        for filename2 in s.derived[ufd]:
            print rdf_derived_fmt.format(
                url_file1 = 'file://'+urllib.pathname2url(filename1),
                url_file2 = 'file://'+urllib.pathname2url(filename2),
            )
        del s.derived[ufd]

    # cleanup generated
    if filename1 in s.generated: s.generated.remove(filename1)

def process_g(data):
    global s
    # line format: g:<gen mode>:<program name>:<filename>

    mode, exename, filename = data.split(':');
    assert s.exe == exename, "Unexpected change to executable name. Expected %s. Got %s." % (s.exe, exename)

    if mode == 't' or mode == 'g':
        print rdf_generated_fmt.format(
            url_program = 'file://'+urllib.pathname2url(s.exe),
            url_file = 'file://'+urllib.pathname2url(filename),
        )
    else:
        #do not generate triple yet - it will be generated on first write
        s.generated.add(filename);

def process_o(data):
    global s
    # line format: o:<ufd>:<filename>
    ufd, filename = data.split(':')
    s.ufdmap[ufd] = filename

    # print triple
    print rdf_open_fmt.format(
        url_file='file://'+urllib.pathname2url(filename),
        label=filename
    )

def process_u(data):
    global s
    # line format: g:<program name>:<filename>
    exename, filename = data.split(':')
    assert s.exe == exename, "Unexpected change to executable name. Expected %s. Got %s." % (s.exe, exename)

    #print triple
    print rdf_used_fmt.format(
        url_program = 'file://'+urllib.pathname2url(exename),
        url_file = 'file://'+urllib.pathname2url(filename),
    )

def process_w(data):
    global s
    # line format: w:<range type>:<output ufd>:<output offset>:<origin ufd>:<origin offset>:<length>
    rtype, ufd, offset, ufd_origin, offset_origin, length = data.split(':', 5)

    if ufd not in s.ufdmap:
        raise UnknownUFDError(ufd)
    if ufd_origin not in s.ufdmap:
        raise UnknownUFDError(ufd_origin)

    filename = s.ufdmap[ufd]
    filename_origin = s.ufdmap[ufd_origin]
    offset = int(offset)
    offset_origin = int(offset_origin)
    length = int(length)

    # emit generated triple if needed
    if filename in s.generated:
        print rdf_generated_fmt.format(
            url_program = 'file://'+urllib.pathname2url(s.exe),
            url_file = 'file://'+urllib.pathname2url(filename),
        )
        s.generated.remove(filename)

    # simple file provenance
    if ufd in s.derived:
        s.derived[ufd].add(filename_origin)
    else:
        s.derived[ufd] = set([filename_origin])

    # output ranges
    if s.minrange > 0 and length >= s.minrange:
        if rtype == 'SEQ':
            print rdf_member_fmt.format(
                url_file = 'file://'+urllib.pathname2url(filename),
                range = file_range_fmt % (offset, offset+length-1)
            )
            print rdf_member_fmt.format(
                url_file = 'file://'+urllib.pathname2url(filename_origin),
                range = file_range_fmt % (offset_origin, offset_origin+length-1)
            )
            print rdf_derived_range_fmt.format(
                url_file1 = 'file://'+urllib.pathname2url(filename),
                range1 = file_range_fmt % (offset, offset+length-1),
                url_file2 = 'file://'+urllib.pathname2url(filename_origin),
                range2 = file_range_fmt % (offset_origin, offset_origin+length-1)
            )
        elif rtype == 'REP':
            print rdf_member_fmt.format(
                url_file = 'file://'+urllib.pathname2url(filename),
                range = file_range_fmt % (offset, offset+length-1)
            )
            print rdf_member_fmt.format(
                url_file = 'file://'+urllib.pathname2url(filename_origin),
                range = file_range_fmt % (offset_origin, offset_origin)
            )
            print rdf_derived_range_fmt.format(
                url_file1 = 'file://'+urllib.pathname2url(filename),
                range1 = file_range_fmt % (offset, offset+length-1),
                url_file2 = 'file://'+urllib.pathname2url(filename_origin),
                range2 = file_range_fmt % (offset_origin, offset_origin)
            )

    # TODO: Aggregation per written buffer is done inside dtracker.
    # Additional aggregation may be done here.

def process_x(data):
    # line format: x:<program name>
    global s
    s.exe = data
    s.generated.clear()

    print rdf_exec_fmt.format(
        url_program = 'file://'+urllib.pathname2url(s.exe),
    )



class Raw2TTLState:
    exe = None
    ufdmap = {}
    derived = {}
    generated = set()
    minrange = 0

#### main ###########################################################
if __name__ == "__main__":
    s = Raw2TTLState()
    tag_range = {}

    parser = argparse.ArgumentParser(description='Convert DataTracker raw format to PROV/Turtle format.')
    parser.add_argument('-minrange', type=int, default=0, help='the minimum range size to be included in the output')
    parser.add_argument('files', metavar='file', nargs='*', help='specify input files')
    args = parser.parse_args()
    s.minrange = args.minrange

    print rdf_header
    for line in fileinput.input(args.files):
        op, data =  line.strip().split(':', 1)

        try:
            globals()['process_'+op](data)
        except KeyError:
            # Keep bad line as comments
            if op.startswith("#"):
                print line.strip()
            else:
                print '#'+line.strip()
                raise
