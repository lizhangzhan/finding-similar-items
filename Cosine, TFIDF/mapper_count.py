import sys
from mrjob.protocol import JSONValueProtocol
from mrjob.job import MRJob
import re
WORD_RE = re.compile(r"[\w']+")
class MRWordCount(MRJob):
    #INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = JSONValueProtocol

    def mapper(self, key, email):
        for term in WORD_RE.findall(email):
			print term
			yield term, 1

    def reducer(self, term, howmany):
        yield None, {'term': term, 'count': sum(howmany)}

if __name__ == '__main__':
        MRWordCount.run()
