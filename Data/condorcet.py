from mrjob.job import MRJob
from mrjob.step import MRStep

class Condorcet(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def map1(self, _, line):
        (first, second, third) = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    Condorcet.run()
