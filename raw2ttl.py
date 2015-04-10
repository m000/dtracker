#!/usr/bin/env python

# Converter from our custom raw provenance output to turtle output.
# See: http://www.w3.org/TeamSubmission/turtle/  
#
# The classes contained here also serve as a base for building
# converters for other formats.

from abc import ABCMeta

import argparse
import sys
import fileinput
import string
import urllib
import inspect
from textwrap import dedent
from pprint import pprint

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


#### classes for used data types ####################################
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


#### converter classes ##############################################
class RawConverter:
	__metaclass__ = ABCMeta
	formats = {}
	exe = None
	ufdmap = {}
	derived = {}
	generated = set()

	def __init__(self, minrange=0):
		self.minrange = minrange
		self.output_static('header')
		self.handlers = dict(filter(
			lambda t: t[0].startswith('handle_'),
			inspect.getmembers(self, predicate=inspect.ismethod)
		))

	def format(self, fmt, **kwargs):
		return self.formats[fmt].format(**kwargs)

	def output_static(self, what):
		print self.formats[what]

	def output_format(self, fmt, **kwargs):
		print self.formats(fmt).format(**kwargs)

	def process_line(self, line):
		line = line.strip()

		if line.startswith('#'):
			print line
		else:
			op, data =  line.strip().split(':', 1)
			try:
			    self.handlers['handle_'+op](data)
			except KeyError:
				# Keep bad lines as comments
				print '# '+line
				raise


class RawTTLConverter(RawConverter):
	formats = {
		'header': dedent('''
		    @prefix prov: <http://www.w3.org/ns/prov#> .
		    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
		''').strip(),
		'rdf_derived': dedent('''
		    <{url_file1}> prov:wasDerivedFrom <{url_file2}> .
		''').strip(),
		'rdf_derived_range': dedent('''
		    <{url_file1}{range1}> prov:wasDerivedFrom <{url_file2}{range2}> .
		''').strip(),
		'rdf_exec': dedent('''
		    <{url_program}> a prov:Activity .
		''').strip(),
		'rdf_generated': dedent('''
		    <{url_file}> prov:wasGeneratedBy <{url_program}> .
		''').strip(),
		'rdf_member': dedent('''
		    <{url_file}> prov:hadMember <{url_file}{range}> .
		''').strip(),
		'rdf_open': dedent('''
		    <{url_file}> a prov:Entity .
		    <{url_file}> rdfs:label "{label}" .
		''').strip(),
		'rdf_used': dedent('''
		    <{url_program}> prov:used <{url_file}> .
		''').strip(),
		'file_range': '#%d-%d',
	}

	def handle_c(self, data):
	    global s
	    # line format: c:<ufd>
	    ufd = data
	    filename1 = self.ufdmap[ufd]

	    # print triples
	    if ufd in self.derived:
	        for filename2 in self.derived[ufd]:
	            print self.format('rdf_derived',
	                url_file1 = 'file://'+urllib.pathname2url(filename1),
	                url_file2 = 'file://'+urllib.pathname2url(filename2),
	            )
	        del self.derived[ufd]

	    # cleanup generated
	    if filename1 in self.generated: self.generated.remove(filename1)

	def handle_g(self, data):
	    global s
	    # line format: g:<gen mode>:<program name>:<filename>

	    mode, exename, filename = data.split(':');
	    assert self.exe == exename, "Unexpected change to executable name. Expected %s. Got %s." % (self.exe, exename)

	    if mode == 't' or mode == 'g':
	        print self.format('rdf_generated',
	            url_program = 'file://'+urllib.pathname2url(self.exe),
	            url_file = 'file://'+urllib.pathname2url(filename),
	        )
	    else:
	        #do not generate triple yet - it will be generated on first write
	        self.generated.add(filename);

	def handle_o(self, data):
	    global s
	    # line format: o:<ufd>:<filename>
	    ufd, filename = data.split(':')
	    self.ufdmap[ufd] = filename

	    # print triple
	    print self.format('rdf_open',
	        url_file='file://'+urllib.pathname2url(filename),
	        label=filename
	    )

	def handle_u(self, data):
	    global s
	    # line format: g:<program name>:<filename>
	    exename, filename = data.split(':')
	    assert self.exe == exename, "Unexpected change to executable name. Expected %s. Got %s." % (self.exe, exename)

	    #print triple
	    print self.format('rdf_used',
	        url_program = 'file://'+urllib.pathname2url(exename),
	        url_file = 'file://'+urllib.pathname2url(filename),
	    )

	def handle_w(self, data):
	    global s
	    # line format: w:<range type>:<output ufd>:<output offset>:<origin ufd>:<origin offset>:<length>
	    rtype, ufd, offset, ufd_origin, offset_origin, length = data.split(':', 5)

	    if ufd not in self.ufdmap:
	        raise UnknownUFDError(ufd)
	    if ufd_origin not in self.ufdmap:
	        raise UnknownUFDError(ufd_origin)

	    filename = self.ufdmap[ufd]
	    filename_origin = self.ufdmap[ufd_origin]
	    offset = int(offset)
	    offset_origin = int(offset_origin)
	    length = int(length)

	    # emit generated triple if needed
	    if filename in self.generated:
	        print self.format('rdf_generated',
	            url_program = 'file://'+urllib.pathname2url(self.exe),
	            url_file = 'file://'+urllib.pathname2url(filename),
	        )
	        self.generated.remove(filename)

	    # simple file provenance
	    if ufd in self.derived:
	        self.derived[ufd].add(filename_origin)
	    else:
	        self.derived[ufd] = set([filename_origin])

	    # output ranges
	    if self.minrange > 0 and length >= self.minrange:
	        if rtype == 'SEQ':
	            print self.format('rdf_member',
	                url_file = 'file://'+urllib.pathname2url(filename),
	                range = file_range_fmt % (offset, offset+length-1)
	            )
	            print self.format('rdf_member',
	                url_file = 'file://'+urllib.pathname2url(filename_origin),
	                range = file_range_fmt % (offset_origin, offset_origin+length-1)
	            )
	            print self.format('rdf_derived_range',
	                url_file1 = 'file://'+urllib.pathname2url(filename),
	                range1 = file_range_fmt % (offset, offset+length-1),
	                url_file2 = 'file://'+urllib.pathname2url(filename_origin),
	                range2 = file_range_fmt % (offset_origin, offset_origin+length-1)
	            )
	        elif rtype == 'REP':
	            print self.format('rdf_member',
	                url_file = 'file://'+urllib.pathname2url(filename),
	                range = file_range_fmt % (offset, offset+length-1)
	            )
	            print self.format('rdf_member',
	                url_file = 'file://'+urllib.pathname2url(filename_origin),
	                range = file_range_fmt % (offset_origin, offset_origin)
	            )
	            print self.format('rdf_derived_range',
	                url_file1 = 'file://'+urllib.pathname2url(filename),
	                range1 = file_range_fmt % (offset, offset+length-1),
	                url_file2 = 'file://'+urllib.pathname2url(filename_origin),
	                range2 = file_range_fmt % (offset_origin, offset_origin)
	            )

	    # TODO: Aggregation per written buffer is done inside dtracker.
	    # Additional aggregation may be done here.

	def handle_x(self, data):
	    # line format: x:<program name>
	    global s
	    self.exe = data
	    self.generated.clear()

	    print self.format('rdf_exec',
	        url_program = 'file://'+urllib.pathname2url(self.exe),
	    )


#### main ###########################################################
if __name__ == "__main__":
    tag_range = {}

    parser = argparse.ArgumentParser(description='Convert DataTracker raw format to PROV/Turtle format.')
    parser.add_argument('-minrange', type=int, default=0, help='the minimum range size to be included in the output')
    parser.add_argument('files', metavar='file', nargs='*', help='specify input files')
    args = parser.parse_args()

    converter = RawTTLConverter(minrange = args.minrange)

    for line in fileinput.input(args.files):
		converter.process_line(line)
