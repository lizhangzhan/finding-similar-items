from mrjob.job import MRJob
import re
import csv
import numpy as np
import sys
from mrjob.protocol import JSONValueProtocol
import string
import math

WORD_RE = re.compile(r"[\w']+")
output_words = []

class MRIDFCount(MRJob):
    #INPUT_PROTOCOL = JSONValueProtocol
    #OUTPUT_PROTOCOL = JSONValueProtocol
    def mapper(self, _, line):
		for docid, b, c, d in csv.reader(line.split('\n'), delimiter=','):
			for word in set(WORD_RE.findall(b)):
				word = word.lower()
				yield word, 1

    def reducer(self, word, values):
		total = sum(values)
		yield word, math.log(364504.0/total, 10)

if __name__ == '__main__':
	MRIDFCount.run()
