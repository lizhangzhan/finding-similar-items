#from nltk.corpus import stopwords
import numpy as np
import re
import sys
import math

WORD_RE = re.compile(r"[\w']+")
string = sys.argv[1]
print "Given String: "+string
#stopword = stopwords.words('english')

print "Loading tf_all_docs..."
tfarray = np.load('mapper_tf_all_docs_original.npy')

print "Loading idf..."
idfarray = np.load('idf.npy')

print "Loading countlist..."
countlist = np.load('countlist.npy')

#print "Loading tfidfvalues..."
#array = np.load('tfidfvalues.npy')

print "Loading lengths with tfidf..."
docidfarray = np.load('tfidf_frequencies.npy')

finallist=[]
content = WORD_RE.findall(string)
lengthofstring = len(content)
wordlist = set()
doclist = set()
queryvector = [[-1,-1]]
cosvalues = [[-1,-1]]

#building the query vector
print "Building query vector..."
for word in [item.lower() for item in content]:
	if(word in tfarray[:,0]):
		queryarray = np.array(queryvector, dtype=object)
		if(word not in queryarray[:,0]):
			queryvector.append([word, 1])
		else:
			index = np.where(queryarray[:0]==word)
			index = index[0][0]
			queryvector[index][1] += 1
queryvector[0][0] = '-1'
queryarray = np.array(queryvector, dtype=object)
queryarray = queryarray[1:len(queryarray),:]
print "The query array is..."
print queryarray

print "Calculating the square root values..."
ans = 0
idf = 0
for j in range(0, len(queryarray)):
	word = queryarray[j][0]
	count = queryarray[j][1]
	try:
		index = np.where(idfarray[:,0]==word)
		index = index[0][0]
		idf = idfarray[index][1]
		ans += (count*idf)**2
	except:
		continue
querysqrtvalue = math.sqrt(ans)
print "Sqrt value:" + str(querysqrtvalue)

print "Searching for similar queries..."
for j in range(0, len(queryarray)):
	word = queryarray[j][0]
	index = np.where(tfarray[:,0]==word)
	index = index[0][0]
	idfval = idfarray[index][1]
	docarray = np.array(tfarray[index][1], dtype=object)
	#print docarray[:,0]
	for i in range(0, len(docarray)):
		cosarray = np.array(cosvalues, dtype=object)
		if(docarray[i][0] not in cosarray[:,0]):
			cosvalues.append([docarray[i][0], idfval*idfval*queryarray[j][1]*docarray[i][1]])
		else:
			index1 = np.where(cosarray[:,0] == docarray[i][0].encode('utf8', errors='replace'))
			try:
				index1 = index1[0][0]
				cindex = np.where(countlist[:,0] == docarray[i][0].encode('utf8', errors='replace'))
				cindex = cindex[0][0]
				cosvalues[index1][1] += (idfval*idfval*queryarray[j][1]*docarray[i][1])
			except:
				continue
cosarray = np.array(cosvalues, dtype=object)
cosarray = cosarray[1:len(cosarray),:]
for i in range(0, len(cosarray)):
	docid = cosarray[i][0]
	index = np.where(docidfarray[:,0]==docid)
	try:
		index = index[0][0]
		sqrtdoc = docidfarray[index][1]
		cosarray[i][1] /= (querysqrtvalue * sqrtdoc)
	except:
		continue

# Sorting the array to get the maximum
cosarray = cosarray[cosarray[:,1].argsort()]
cosarraylen = len(cosarray)
print cosarraylen
print cosarray
print cosarray[cosarraylen-1]
indextest = np.where(countlist[:,0]=="1754275")
indextest = indextest[0][0]
print countlist[indextest]


print "Top 20 results:"
for i in range(0, 20):
	index = np.where(countlist[:,0]==cosarray[cosarraylen-1-i][0].encode('utf8', errors='replace'))
	index = index[0][0]
	#print countlist[index][2] + " Length:" + str(docidfarray[index][1])
	print countlist[index][2] + "  Cosine similarity:" + str(cosarray[cosarraylen-1-i][1])
"""
allwords = np.load('allwordsarray.npy')
queryvector = [[-1,-1]]
docs = []
wordsarray = []
cosinevalues = []
for word in content:
	word = word.lower()
	if(word in allwords):
		queryarray = np.array(queryvector)
		index = np.where(allwords == word)
		index = index[0][0]
		if(index not in wordsarray):
			wordsarray.append(index)

		# building the query vector
		if(index not in queryarray[:,0]):
			queryvector.append([index,1])
		else:
			wordindex = np.where(queryarray[:,0] == index)
			wordindex = wordindex[0][0]
			queryvector[wordindex][1] += 1

# Taking the different documents that need to be compared
for i in range(1,10):
	filename = "finaltfidf"+str(i)+".npy"
	print filename
	tfidfarray = np.load(filename)
	for index in wordsarray:
		docarray = np.array(tfidfarray[index])
		for docindex in docarray[:,0]:
			if(docindex not in docs):
				docs.append(docindex)

# For getting the tf of different docs
array1 = np.load("finaltf1.npy")
array2 = np.load("finaltf2.npy")
array3 = np.load("finaltf3.npy")
array4 = np.load("finaltf4.npy")
array5 = np.load("finaltf5.npy")
array6 = np.load("finaltf6.npy")
array7 = np.load("finaltf7.npy")
array8 = np.load("finaltf8.npy")
array9 = np.load("finaltf9.npy")
queryvec = np.array(queryvector)
for i in docs:
	if(i<13678):
		#search in file 1
		array = array1[i]
	else:
		if(i<13678+33730):
			#search in file 2
			array = array2[i-(13678)]
		else:
			if(i<13678+33730+40763):
				#search in file 3
				array = array3[i-(13678+33730)]
			else:
				if(i<13678+33730+40763+42782):
					#search in file 4
					array = array4[i-(13678+33730+40763)]
				else:
					if(i<13678+33730+40763+42782+44495):
						#search in file 5
						array = array5[i-(13678+33730+40763+42782)]
					else:
						if(i<13678+33730+40763+42782+44495+46616):
							#search in file 6
							array = array6[i-(13678+33730+40763+42782+44495)]
						else:
							if(i<13678+33730+40763+42782+44495+46616+47860):
								#search in file 7
								array = array7[i-(13678+33730+40763+42782+44495+46616)]
							else:
								if(i<13678+33730+40763+42782+44495+46616+47860+47222):
									#search in file 8
									array = array8[i-(13678+33730+40763+42782+44495+46616+47860)]
								else:
									#search in file 9
									array = array9[i-(13678+33730+40763+42782+44495+46616+47860+47222)]
	value = 0
	j=0
	array = np.array(array)
	print i #printing out docs
	for wordindex in queryvec[:,0]:
		if((wordindex in array[:,0]) and (wordindex!=-1)):
			index_array = np.where(array[:,0] == wordindex)
			index_array = index_array[0][0]
			value += queryvec[j,1]*array[index_array][1]
		j += 1
	cosinevalues.append([i, value])
cosinevalues = np.array(cosinevalues)
cosinevalues = cosinevalues[cosinevalues[:,1].argsort()]
np.savetxt('cosinevalues.txt', cosinevalues, delimiter=" ", fmt="%s")
np.save('cosinevalues.npy', cosinevalues)
print len(cosinevalues)
print queryvector
print content
print len(docs)"""
