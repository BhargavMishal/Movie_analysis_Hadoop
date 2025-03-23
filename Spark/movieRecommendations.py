from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

# function to create a python dictionary for the data
# we will use it later to get movie names from movieIDs
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# converting each line from u.data into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # creating a spark session
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()
    # loading up the movie name lookup dictionary
    movieNames = loadMovieNames()
    # getting the raw data
    lines = spark.read.text("hdfs:///user/bhargav/ml-100k/u.data").rdd
    # converting it to an RDD of Row objects having (userID, movieID, rating) rows
    ratingsRDD = lines.map(parseInput)
    # converting it to a DataFrame and the caching it
    ratings = spark.createDataFrame(ratingsRDD).cache()

    # creating an ALS colloborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # printing ratings from user 0
    print("Ratings from user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print(movieNames[rating['movieID']], rating['rating'])
    
    # printing top 20 recommendations
    print() # leaving a line
    print("Top 20 recommendations:")
    # finding movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # constructing a test dataframe for user 0 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))
    # running our model on this list
    recommendations = model.transform(popularMovies)
    # getting the top 20 out of these recommendations
    topRecs = recommendations.sort(recommendations.prediction.desc()).take(20)
    for rec in topRecs:
        print(movieNames[rec['movie']], rec['prediction'])

    spark.stop()