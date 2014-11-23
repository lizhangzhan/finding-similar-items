import json
from mrjob.job import MRJob
from pprint import pprint

char_space = 'abcdefghijklmnopqrstuvwxyz' # + ' 1234567890.,+-*/\'"()_\n!@#$%^&=:?;<>[]\t\\|{}'
char_space_size = len(char_space)
char_id = {c:i for i,c in enumerate(char_space)}
shingle_size = 5
shingle_id = {}
doc_id = {}

def hash_shingle(shingle):
    """Returns a base-26 number represented in base-10.
    26 is the size of char_space. Might go higher if more characters considered.
    """
    # Append a's if shingle length less than shingle_size.
    shingle += (shingle_size-len(shingle)) * 'a'
    h = 0
    l = char_space_size
    for i in xrange(shingle_size):
        h += (l ** i) * char_id[shingle[i]]
    return h

class FindSimilarity(MRJob):
    def mapper0(self, _, line):
        email = json.loads(line)
        date = email['date']
        text = email['text'].lower()
        text = ''.join([c for c in text if c in char_space])
        # Shingle size is 5.
        shingles = []
        for i in xrange(len(text)-4):
            shingle = text[i:i+shingle_size]
            _hash = hash_shingle(shingle)
            if _hash not in shingle_id:
                shingle_id[_hash] = shingle
            shingles.append(shingle)
        yield 1, (hash(date), shingles)

    def reducer0(self, _, docs):
        shingles = set()
        docs = list(docs)
        for _, doc_shingles in docs:
            shingles.update(set(doc_shingles))
        shingles = list(shingles)
        shingles.sort()
        count = 0
        for doc_ts, doc_shingles in docs:
            yield (count, doc_ts, doc_shingles), shingles
            count += 1

    def mapper1(self, doc, shingles):
        sig_len = 10
        col = [-1] * sig_len
        doc_id, doc_ts, doc_shingles = doc
        shingles_len = len(shingles)
        for i in xrange(shingles_len):
            if shingles[i] in doc_shingles:
                for j in xrange(sig_len):
                    if col[j] == -1:
                        col[j] = (i*2*j+i+1) % shingles_len
                    else:
                        col[j] = min(col[j], (i*2*j+i+1) % shingles_len)

        yield None, ((doc_id, doc_ts), col)

    def reducer1(self, _, docs_and_col):
        docs_and_col = list(docs_and_col)
        sig_mat_rows = len(docs_and_col[0][1])
        sig_mat_cols = len(docs_and_col)
        sig_mat = [[0 for i in xrange(sig_mat_cols)] for j in xrange(sig_mat_rows)]
        i = 0
        docs = dict()
        mx_minhash = 0
        for doc_col in docs_and_col:
            mx_minhash = max(mx_minhash, max(doc_col[1]))
            doc_id, doc_ts = doc_col[0]
            col = doc_col[1]
            for j in xrange(sig_mat_rows):
                sig_mat[j][i] = col[j]
            i += 1
            docs[doc_id] = doc_ts

        num_docs = len(docs)
        # LSH implemented here.
        # Give each mapper one band to deal with.
        # depending on the # of bands, results may vary
        # try values 2 to 4
        bands = 4
        sig_len = len(sig_mat[0])
        temp = sig_len / bands

        pprint(sig_mat)
        print docs

        for i in xrange(bands):
            yield i, (mx_minhash, sig_mat[i:(i+1)*temp], docs)
    
    def mapper2(self, band_id, hash_maxval_band_and_doc_ids):
        hash_maxval, band, doc_ids = hash_maxval_band_and_doc_ids
        hmx = hash_maxval # shortform!
        buckets = dict()
        num_docs = len(band[0])
        band_len = len(band)
        for i in xrange(num_docs):
            doc_hash = 1
            for j in xrange(band_len):
                doc_hash = (doc_hash + band[j][i] * hmx) % 1000000007
            if not buckets.has_key(doc_hash):
                buckets[doc_hash] = []
            buckets[doc_hash].append(doc_ids[str(i)])

        for k, v in buckets.items():
            if len(v) > 1:
                for i in xrange(len(v)-1):
                    for j in xrange(i+1, len(v)):
                        yield None, (v[i], v[j])

    def reducer2(self, _, doc_sims):
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
