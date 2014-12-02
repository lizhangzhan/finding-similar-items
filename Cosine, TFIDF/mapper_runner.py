#from mapper_tf import MRWordFrequencyCount# only needed on Python 2.5
from mapper_tf_all_docs import MRWordFrequencyCount
import numpy as np
import csv

mr_job = MRWordFrequencyCount(args=["QueryResults_inorder.csv"])
idf = []
tfidfvalues=[]

"""
with open('mapper_idf.txt', 'rb') as csvfile:
	reader = csv.reader(csvfile, delimiter="\t")
	t=0
	for row in reader:
		idf.append([row[0], float(row[1])])
		t+=1
	print "idf:"+str(t)
idf = np.array(idf, dtype=object)
print "idf done"
np.save('idf.npy', idf)
"""

with mr_job.make_runner() as runner:
	runner.run()	
	l=0
	for line in runner.stream_output():
		key, value = mr_job.parse_output_line(line)
		#a = np.array(value)
		#a = list(value)
		tfidfvalues.append([key, value])

print tfidfvalues[0]
tfidfvalues = np.array(tfidfvalues, dtype=object)
np.save('mapper_tf_all_docs_original.npy', tfidfvalues)
