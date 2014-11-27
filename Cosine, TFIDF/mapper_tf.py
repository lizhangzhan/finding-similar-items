from mrjob.job import MRJob
import re
import csv
import numpy as np
import sys
from mrjob.protocol import JSONValueProtocol
import string
WORD_RE = re.compile(r"[\w']+")
class MRWordFrequencyCount(MRJob):
   # INPUT_PROTOCOL = JSONValueProtocol
    #OUTPUT_PROTOCOL = JSONValueProtocol
    def mapper(self, _, line):
		for docid, b, c, d in csv.reader(line.split('\n'), delimiter=','):
			for word in WORD_RE.findall(b):
				word = word.lower()
				yield {'word': word, 'docid': docid}, 1
    def combiner(self, word_docid, values):
		ans = sum(values)
		print word_docid['word']
		yield word_docid['docid'].encode('utf8', errors='replace'), (word_docid['word'].encode('utf8', errors='replace'), ans)
    def reducer(self, docid, value):
		yield docid, list(value)

if __name__ == '__main__':
	MRWordFrequencyCount.run()
	#print output_words
	#for line in MRWordFrequencyCount.stream_output():
	#key, value = MRWordFrequencyCount.parse_output_line(line)
	#print value
