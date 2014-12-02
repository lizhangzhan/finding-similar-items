from mrjob.job import MRJob
import re
import csv
import numpy as np
import sys
from mrjob.protocol import JSONValueProtocol
import string

WORD_RE = re.compile(r"[\w']+")
output_words = []

class MRWordFrequencyCount(MRJob):
    #INPUT_PROTOCOL = JSONValueProtocol
    #OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, _, line):
		for docid, b, c, d in csv.reader(line.split('\n'), delimiter=','):
			for word in set(WORD_RE.findall(b)):
				word = word.lower()
				yield word, docid

    def reducer(self, word, docid):
		yield word, set(docid)

if __name__ == '__main__':
	MRWordFrequencyCount.run()
