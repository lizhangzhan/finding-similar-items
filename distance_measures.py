# Implementation of some distance measures.
# Namely, euclidean, jaccard, cosine, edit and hamming distances.

# Helper functions.

# Interface.
def distance_euclidean():
    pass

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
