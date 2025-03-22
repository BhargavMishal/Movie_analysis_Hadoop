# using map reduce (mrjob library in python)
# used to get movies in order of their popularity (count of ratings) least to most

from mrjob.job import MRJob
from mrjob.step import MRStep

class PopularityMovies (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sorted_movies)
        ]
    
    def mapper_get_ratings(self, line):
        # maps value 1 to movieID, i.e movieID --> 1
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_ratings(self, key, values):
        # reduces mapped movieIDs to their totals
        yield str(sum(values)).zfill(5), key # padding with 0

    def reducer_sorted_movies(self, count, movies):
        # auto sorts the movies based on the first value (total ratings here)
        for movie in movies:
            yield movie, count

if __name__ == '__main__':
    PopularityMovies.run()