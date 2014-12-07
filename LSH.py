import json
from mrjob.job import MRJob
from sys import maxint

char_space = 'abcdefghijklmnopqrstuvwxyz' # + ' 1234567890.,+-*/\'"()_\n!@#$%^&=:?;<>[]\t\\|{}'
char_space_size = len(char_space)
char_id = {c:i for i,c in enumerate(char_space)}
shingle_size = 5
shingle_id = {}
doc_id = {}

# Time for some refactoring.

# Functions.
unique_id = 0
def get_unique_id():
    """Returns a unique identifier. Based on a global counter."""
    global unique_id
    unique_id += 1
    return unique_id

def hash_shingle(shingle):
    """Returns a base-26 number represented in base-10.
    26 is the size of char_space. Might go higher if more characters considered."""
    # Append a's if shingle length less than shingle_size.
    shingle += (shingle_size-len(shingle)) * 'a'
    h = 0
    l = char_space_size
    for i in xrange(shingle_size):
        h += (l ** i) * char_id[shingle[i]]
    return h

def generate_shingles(text, shingle_len):
    """Take a string <text> and a number <shingle_len>.
    Return the set of all it's shingles of length <shingle_len>."""
    shingles = set()
    for idx in xrange(1+len(text)-shingle_len):
        shingle = text[idx:idx+shingle_len]
        shingles.add(shingle)
    return shingles

class FindSimilarity(MRJob):
    def mapper0(self, _, line):
        """Yields a (date_hash, shingles) pair for a document."""
        doc = json.loads(line)

        id = get_unique_id()
        text = doc['text'].lower()
        text = ''.join([c for c in text if c in char_space])
        shingles = list(generate_shingles(text, shingle_size))

        # Store newfound shingles in global dict shingle_id.
        for shingle in shingles:
            shingle_id.setdefault(hash_shingle(shingle), shingle)

        yield 1, (hash(id), shingles)

    def reducer0(self, _, docs):
        """Yields a (id, doc_timestamp, doc_shingles) and
        the global shingle set for each doc.
        """
        all_shingles = set()
        docs = list(docs)
        for _, doc_shingles in docs:
            all_shingles.update(set(doc_shingles))
        all_shingles = list(all_shingles)
        all_shingles.sort()
        for i, doc in enumerate(docs):
            yield [i] + doc, all_shingles

    def mapper1(self, doc, shingles):
        """For each doc, compute and return signature from it's shingles"""
        sig_len = 10
        sig = [maxint] * sig_len
        doc_id, doc_ts, doc_shingles = doc
        shingles_len = len(shingles)

        for i, shingle in enumerate(shingles):
            if shingle in doc_shingles:
                for j in xrange(sig_len):
                    hash_val = (i*j*2 + i+1) % shingles_len
                    sig[j] = min(sig[j], hash_val)

        yield None, ((doc_id, doc_ts), sig)

    def reducer1(self, _, docs_and_sigs):
        """Receive all docs with their signatures.
        Return part of signature matrix for all docs to each band."""
        docs_and_sigs = list(docs_and_sigs)
        sigmat_rows = len(docs_and_sigs[0][1]) # = sig_len
        sigmat_cols = len(docs_and_sigs)
        sigmat = [[0 for i in xrange(sigmat_cols)] for j in xrange(sigmat_rows)]

        i = 0
        docs = {}
        mx_minhash = 0
        # Construct signature matrix.
        for doc_and_sig in docs_and_sigs:
            mx_minhash = max(mx_minhash, max(doc_and_sig[1]))
            doc_id, doc_ts = doc_and_sig[0]
            sig = doc_and_sig[1]
            for j in xrange(sigmat_rows):
                sigmat[j][i] = sig[j]
            i += 1
            docs[doc_id] = doc_ts

        # LSH implemented here.
        # Give each mapper one band to deal with.
        # depending on the # of bands, results may vary
        # try values 2 to 4
        bands = 4
        sig_len = len(sigmat[0])
        rows = sig_len / bands

        for i in xrange(bands):
            rows_in_band = sigmat[i*rows:(i+1)*rows]
            yield i, (mx_minhash, rows_in_band, docs)
    
    def mapper2(self, band_id, hash_maxval_band_and_doc_ids):
        """Receive part of doc's minhashes and compute band similarity."""
        hash_maxval, band, doc_ids = hash_maxval_band_and_doc_ids
        hmx = hash_maxval # shortform!

        buckets = {}
        num_docs = len(band[0])
        band_len = len(band)

        for i in xrange(num_docs):
            # For each doc, compute hash and append to a hash->doc map.
            doc_hash = 1
            for j in xrange(band_len):
                doc_hash = (doc_hash + band[j][i] * hmx) % 1000000007
            buckets.setdefault(doc_hash, [])
            buckets[doc_hash].append(doc_ids[str(i)])

        # Yield items in the same bucket, since they are similar.
        for k, v in buckets.items():
            if len(v) > 1:
                for i in xrange(len(v)-1):
                    for j in xrange(i+1, len(v)):
                        yield None, (v[i], v[j])

    def reducer2(self, _, doc_sims):
        """Yield each pair from a list of all pairs."""
        for doc_sim in doc_sims:
            yield None, doc_sim
    
    def steps(self):
        return [
            self.mr(mapper=self.mapper0, reducer=self.reducer0),
            self.mr(mapper=self.mapper1, reducer=self.reducer1),
            self.mr(mapper=self.mapper2, reducer=self.reducer2)
        ]

if __name__ == '__main__':
    FindSimilarity.run()
