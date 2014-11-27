from nltk.corpus import stopwords
import numpy as np
string = "Setting title of a page"
stopword = stopwords.words('english')

content = [w for w in string.split() if w.lower() not in stopword]

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
print len(docs)