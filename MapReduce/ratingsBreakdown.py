# using map reduce (mrjob library in python)
# used to get no. of movies according to rating

from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]
    
    def mapper_get_ratings(self, line):
        # maps value 1 to rating, i.e rating_val --> 1
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer_count_ratings(self, key, values):
        # reduces mapped rating to their totals
        yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()