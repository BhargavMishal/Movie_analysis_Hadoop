from pyspark import SparkConf, SparkContext

# function to create a python dictionary for the data
# we will use it later to get movie names from movieIDs
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# take each line from u.data and convert it to (movieID (rating, 1.0))
# will add rating for each movie and 1 for each movie for average
def parseInput(line):
    fields = line.split()
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # main script - creates our SparkContext
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf=conf)
    # loading up our movie ID --> movie name lookup dictionary
    movieNames = loadMovieNames()
    # loading up the raw u.data file
    lines = sc.textFile("hdfs:///user/bhargav/ml-100k/u.data")
    # converting to (movieID, (rating, 1.0)) format
    movieRatings = lines.map(parseInput)
    # reducing to (movieID, (sumOfRatings, totalRatings))
    ratingTotals = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0]+movie2[0], movie1[1]+movie2[1]))
    # mapping to (movieID, averageRating)
    averageRatings = ratingTotals.mapValues(lambda total: total[0]/total[1])
    # sorting it by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])
    # taking the top 10 results
    results = sortedMovies.take(10)
    # printing the result
    for result in results:
        print(movieNames[result[0]], result[1])