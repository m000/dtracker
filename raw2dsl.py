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



#### SPADE DSL converter class ###################################
class RawDSLConverter(RawConverter):
	formats = {
		'exec': dedent('''
			type:Process id:{pid} program:{url_program} pid:X
		''').strip(),
		'open': dedent('''
			type:Artifact id:X file:{filename} label???:"{label}"
		''').strip(),
		'used': dedent('''
			type:Used from:{url_program} to:{filename}
		''').strip(),
		'derived': dedent('''
			type:WasDerivedFrom from:{filename1} o:{filename2}
		''').strip(),
		'generated': dedent('''
			type:wasGeneratedBy from:url_program to:{filename}
		''').strip(),

		# not used for now
		'derived_range': dedent('''
			<{filename1}{range1}> prov:wasDerivedFrom <{filename2}{range2}> .
		''').strip(),
		'member': dedent('''
			<{filename}> prov:hadMember <{filename}{range}> .
		''').strip(),
		'file_range': '#%d-%d',
	}

	def handle_c(self, data):
		ufd = itemgetter('ufd')(data)
		filename1 = self.ufdmap[ufd]

		# print triples
		if ufd in self.derived:
			for filename2 in self.derived[ufd]:
				print self.format('derived',
					filename1 = self.__class__.quote_file(filename1),
					filename2 = self.__class__.quote_file(filename2),
				)
			del self.derived[ufd]

		# cleanup generated
		if filename1 in self.generated: self.generated.remove(filename1)

	def handle_g(self, data):
		mode, exe, filename = itemgetter('mode', 'program', 'file')(data)
		assert self.exe == exe, "Unexpected change to executable name. Expected %s. Got %s." % (self.exe, exe)

		if mode == 't' or mode == 'g':
			print self.format('generated',
				url_program = self.__class__.quote_file(self.exe),
				filename = self.__class__.quote_file(filename),
			)
		else:
			#do not generate triple yet - it will be generated on first write
			self.generated.add(filename);

	def handle_o(self, data):
		ufd, filename = itemgetter('ufd', 'file')(data)
		self.ufdmap[ufd] = filename

		print self.format('open',
			filename = self.__class__.quote_file(filename),
			label = filename
		)

	def handle_u(self, data):
		exe, filename = itemgetter('program', 'file')(data)
		assert self.exe == exe, "Unexpected change to executable name. Expected %s. Got %s." % (self.exe, exe)

		print self.format('used',
			url_program = self.__class__.quote_file(exe),
			filename = self.__class__.quote_file(filename),
		)

	def handle_w(self, data):
		rtype, ufd, offset, origin_ufd, origin_offset, length = itemgetter(
			'range_type', 'out_ufd', 'out_offset', 'origin_ufd', 'origin_offset', 'length'
		)(data)

		if ufd not in self.ufdmap:
			raise UnknownUFDError(ufd)
		if origin_ufd not in self.ufdmap:
			raise UnknownUFDError(origin_ufd)

		filename = self.ufdmap[ufd]
		filename_origin = self.ufdmap[origin_ufd]
		offset = int(offset)
		origin_offset = int(origin_offset)
		length = int(length)

		# emit generated triple if needed
		if filename in self.generated:
			print self.format('generated',
				url_program = self.__class__.quote_file(self.exe),
				filename = self.__class__.quote_file(filename),
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
				print self.format('member',
					filename = self.__class__.quote_file(filename),
					range = file_range_fmt % (offset, offset+length-1)
				)
				print self.format('member',
					filename = self.__class__.quote_file(filename_origin),
					range = file_range_fmt % (origin_offset, origin_offset+length-1)
				)
				print self.format('derived_range',
					filename1 = self.__class__.quote_file(filename),
					range1 = file_range_fmt % (offset, offset+length-1),
					filename2 = self.__class__.quote_file(filename_origin),
					range2 = file_range_fmt % (origin_offset, origin_offset+length-1)
				)
			elif rtype == 'REP':
				print self.format('member',
					filename = self.__class__.quote_file(filename),
					range = file_range_fmt % (offset, offset+length-1)
				)
				print self.format('member',
					filename = self.__class__.quote_file(filename_origin),
					range = file_range_fmt % (origin_offset, origin_offset)
				)
				print self.format('derived_range',
					filename1 = self.__class__.quote_file(filename),
					range1 = file_range_fmt % (offset, offset+length-1),
					filename2 = self.__class__.quote_file(filename_origin),
					range2 = file_range_fmt % (origin_offset, origin_offset)
				)

		# TODO: Aggregation per written buffer is done inside dtracker.
		# Additional aggregation may be done here.

	def handle_x(self, data):
		pid, self.exe = itemgetter('pid', 'program')(data)
		self.generated.clear()

		print self.format('exec',
			pid = pid,
			url_program = self.__class__.quote_file(self.exe),
		)



#### main ###########################################################
if __name__ == "__main__":
	tag_range = {}

	parser = argparse.ArgumentParser(description='Convert DataTracker raw format to input for the SPADE DSL Reporter.')
	# parser.add_argument('-minrange', type=int, default=0, help='the minimum range size to be included in the output')
	# parser.add_argument('dsl-pipe', metavar='pipe', nargs='*', help='location of the SPADE DSL pipe')
	parser.add_argument('files', metavar='file', nargs='*', help='specify input files')
	args = parser.parse_args()

	converter = RawDSLConverter(minrange=getattr(args, 'minrange', 0))

	for line in fileinput.input(args.files):
		converter.process_line(line)
