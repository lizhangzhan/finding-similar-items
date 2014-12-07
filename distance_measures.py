# Implementation of some distance measures.
# Namely, euclidean, jaccard, cosine, edit and hamming distances.
import math

# Helper functions.

# Interface.
def distance_euclidean(doc1, doc2, dimensions, l_norm):
    distance = 0
    n, r = dimensions, l_norm
    for doc1_i, doc2_i in zip(doc1, doc2):
        distance += pow(abs(doc1_i-doc2_i), r)
    return pow(distance, 1.0/r)

def distance_jaccard():
    pass

def distance_cosine():
    pass

def distance_edit():
    pass

def distance_hamming(v_doc1, v_doc2):
    distance = 0
    for i in xrange(len(v_doc1)):
        distance += v_doc1[i] != v_doc2[i]
    return distance
