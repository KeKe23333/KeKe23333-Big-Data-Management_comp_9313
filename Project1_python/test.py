import sys
from mrjob.job import MRJob


class MRWordCount(MRJob):
    def mapper(self, _, line):
        line = line.strip()
        words =line.split()
        for i in words : yield (i, 1)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__=='__main__':
    # sys.stdout = open('result.txt', 'w+')
    MRWordCount.run()
    # sys.stdout.close()