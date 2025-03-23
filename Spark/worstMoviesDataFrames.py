from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# function to create a python dictionary for the data
# we will use it later to get movie names from movieIDs
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# take each line from u.data and convert it to (movieID, rating) rows
def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # creating a spark session
    spark = SparkSession.builder.appName("WorstMovies").getOrCreate()
    # loading up the movie name lookup dictionary
    movieNames = loadMovieNames()
    # getting the raw data
    lines = spark.sparkContext.textFile("hdfs:///user/bhargav/ml-100k/u.data")
    # converting it to a RDD of row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # converting this to a DataFrame
    moviesData = spark.createDataFrame(movies)
    # finding total rating counts
    counts = moviesData.groupBy("movieID").count()
    # finding average rating for each movieID
    averageRatings = moviesData.groupBy("movieID").avg("rating")
    # joining for total rating count and average
    avgAndCounts = counts.join(averageRatings, "movieID")
    # filtering movies rated 10 or fewer times
    avgAndCounts = avgAndCounts.filter("count > 10")
    # pulling the top 10 results
    top10 = avgAndCounts.orderBy("avg(rating)").take(10)
    # printing them while mapping movieID to movie names
    for movie in top10:
        print(movieNames.get(movie[0]), movie[1], movie[2])
    # stopping the session
    spark.stop()