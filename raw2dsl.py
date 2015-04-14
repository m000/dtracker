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

# type:<Agent|Process|Artifact> id:<unique identifier> <key>:<value> ... <key>:<value>
# type:<Used|WasGeneratedBy|WasTriggeredBy|WasDerivedFrom|WasControlledBy> from:<unique identifier> to:<unique identifier> <key>:<value> ... <key>:<value> 



class RawDSLConverter(RawConverter):
	formats = {
		'header': '',
		'dsl_exec': dedent('''
		    type:Process id:X program:<{url_program}> pid:X
		''').strip(),
		'dsl_open': dedent('''
		    type:Artifact id:X file???:<{url_file}> a label???:"{label}"
		''').strip(),
		'dsl_used': dedent('''
		    type:Used from:<{url_program}> to:<{url_file}>
		''').strip(),
		'dsl_derived': dedent('''
		    type:WasDerivedFrom from:<{url_file1}> to:<{url_file2}>
		''').strip(),
		'dsl_generated': dedent('''
		    type:wasGeneratedBy from:<url_program> to:<{url_file}>
		''').strip(),

		# not used for now
		'rdf_derived_range': dedent('''
		    <{url_file1}{range1}> prov:wasDerivedFrom <{url_file2}{range2}> .
		''').strip(),
		'rdf_member': dedent('''
		    <{url_file}> prov:hadMember <{url_file}{range}> .
		''').strip(),
		'file_range': '#%d-%d',
	}

	def handle_c(self, data):
	    # line format: c:<ufd>
	    ufd = data
	    filename1 = self.ufdmap[ufd]

	    # print triples
	    if ufd in self.derived:
	        for filename2 in self.derived[ufd]:
	            print self.format('dsl_derived',
	                url_file1 = self.__class__.quote_file(filename1),
	                url_file2 = self.__class__.quote_file(filename2),
	            )
	        del self.derived[ufd]

	    # cleanup generated
	    if filename1 in self.generated: self.generated.remove(filename1)

	def handle_g(self, data):
	    # line format: g:<gen mode>:<program name>:<filename>

	    mode, exename, filename = data.split(':');
	    assert self.exe == exename, "Unexpected change to executable name. Expected %s. Got %s." % (self.exe, exename)

	    if mode == 't' or mode == 'g':
	        print self.format('dsl_generated',
	            url_program = self.__class__.quote_file(self.exe),
	            url_file = self.__class__.quote_file(filename),
	        )
	    else:
	        #do not generate triple yet - it will be generated on first write
	        self.generated.add(filename);

	def handle_o(self, data):
	    # line format: o:<ufd>:<filename>
	    ufd, filename = data.split(':')
	    self.ufdmap[ufd] = filename

	    # print triple
	    print self.format('dsl_open',
	        url_file=self.__class__.quote_file(filename),
	        label=filename
	    )

	def handle_u(self, data):
	    # line format: g:<program name>:<filename>
	    exename, filename = data.split(':')
	    assert self.exe == exename, "Unexpected change to executable name. Expected %s. Got %s." % (self.exe, exename)

	    #print triple
	    print self.format('dsl_used',
	        url_program = self.__class__.quote_file(exename),
	        url_file = self.__class__.quote_file(filename),
	    )

	def handle_w(self, data):
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
	        print self.format('dsl_generated',
	            url_program = self.__class__.quote_file(self.exe),
	            url_file = self.__class__.quote_file(filename),
	        )
	        self.generated.remove(filename)

	    # simple file provenance
	    if ufd in self.derived:
	        self.derived[ufd].add(filename_origin)
	    else:
	        self.derived[ufd] = set([filename_origin])

	    # output ranges
	    # dead block - minrange is currently always 0 for DSL output
	    if self.minrange > 0 and length >= self.minrange:
	        if rtype == 'SEQ':
	            print self.format('rdf_member',
	                url_file = self.__class__.quote_file(filename),
	                range = file_range_fmt % (offset, offset+length-1)
	            )
	            print self.format('rdf_member',
	                url_file = self.__class__.quote_file(filename_origin),
	                range = file_range_fmt % (offset_origin, offset_origin+length-1)
	            )
	            print self.format('rdf_derived_range',
	                url_file1 = self.__class__.quote_file(filename),
	                range1 = file_range_fmt % (offset, offset+length-1),
	                url_file2 = self.__class__.quote_file(filename_origin),
	                range2 = file_range_fmt % (offset_origin, offset_origin+length-1)
	            )
	        elif rtype == 'REP':
	            print self.format('rdf_member',
	                url_file = self.__class__.quote_file(filename),
	                range = file_range_fmt % (offset, offset+length-1)
	            )
	            print self.format('rdf_member',
	                url_file = self.__class__.quote_file(filename_origin),
	                range = file_range_fmt % (offset_origin, offset_origin)
	            )
	            print self.format('rdf_derived_range',
	                url_file1 = self.__class__.quote_file(filename),
	                range1 = file_range_fmt % (offset, offset+length-1),
	                url_file2 = self.__class__.quote_file(filename_origin),
	                range2 = file_range_fmt % (offset_origin, offset_origin)
	            )

	    # TODO: Aggregation per written buffer is done inside dtracker.
	    # Additional aggregation may be done here.

	def handle_x(self, data):
	    # line format: x:<program name>
	    self.exe = data
	    self.generated.clear()

	    print self.format('dsl_exec',
	        url_program = 'file://'+urllib.pathname2url(self.exe),
	    )



#### main ###########################################################
if __name__ == "__main__":
    tag_range = {}

    parser = argparse.ArgumentParser(description='Convert DataTracker raw format to input for the SPADE DSL Reporter.')
    # parser.add_argument('-minrange', type=int, default=0, help='the minimum range size to be included in the output')
    # parser.add_argument('dsl-pipe', metavar='pipe', nargs='*', help='location of the SPADE DSL pipe')
    parser.add_argument('files', metavar='file', nargs='*', help='specify input files')
    args = parser.parse_args()

    converter = RawDSLConverter(minrange = getattr(args, 'minrange', 0))

    for line in fileinput.input(args.files):
		converter.process_line(line)
