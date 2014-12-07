# Implementation of some distance measures.
# Namely, euclidean, jaccard, cosine, edit and hamming distances.
import math

# Helper functions.
def generate_shingles(content, shingle_len)
    shingles = set()
    for idx in xrange(1+len(content)-shingle_len):
        shingle = content[idx : idx+shingle_len]
        shingles.add(shingle)
    return shingles

# Interface.
def distance_euclidean(doc1, doc2, dimensions, l_norm):
    distance = 0
    n, r = dimensions, l_norm
    for doc1_i, doc2_i in zip(doc1, doc2):
        distance += pow(abs(doc1_i - doc2_i), r)
    return pow(distance, 1.0/r)

def distance_jaccard(content_doc1, content_doc2, shingle_len=5):
    shingles_doc1 = generate_shingles(content_doc1, shingle_len)
    shingles_doc2 = generate_shingles(content_doc2, shingle_len)
    similarity = 1.0 * len(shingles_doc1 & shingles_doc2) /
                       len(shingles_doc1 | shingles_doc2)
    return 1 - similarity

def distance_cosine():
    pass

def distance_edit():
    pass

def distance_hamming(v_doc1, v_doc2):
    distance = 0
    for i in xrange(len(v_doc1)):
        distance += v_doc1[i] != v_doc2[i]
    return distance
