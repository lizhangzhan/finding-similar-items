from mrjob.job import MRJob
import re
import csv
import numpy as np
import sys
from mrjob.protocol import JSONValueProtocol
import string
WORD_RE = re.compile(r"[\w']+")
countlist = []
for docid, b, c, d in csv.reader(open('QueryResults_inorder.csv',"rb"), delimiter=','):
	length = len(WORD_RE.findall(b))
	print length
	countlist.append([docid, length, b])
countlist = np.array(countlist, dtype=object)
np.save('countlist.npy',countlist)