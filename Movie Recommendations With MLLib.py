from pyspark.sql import SparkSession
from pyspark.ml.recommendations import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

def loadMovieNames():
    movieNames = {}
    with open ("ml-100k/u.item") as f:
        for lines in f:
            fields = lines.split('|')
            movieNames[int(fields[0])]=fields[1].decode('ascii','ignore')

    return movieNames;

def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), ratings = float(fields[2]))

if __name__=="__main__":
    # Create SparkSession
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()
    #Load movie names into movieNames
    movieNames = loadMovieNames()
    # Loading u.data file and converting into rdd
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd
    # Create row objects of (userID, movieID, ratings)
    ratingsRdd = lines.map(parseInput)
    # Converting row objects to dataFrame and creating a cache for it
    ratings = spark.createDataFrame(ratingsRdd).cache()

    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingsCol="ratings")
    model = als.fit(ratings)

    print("\nRatings for user 0:")
    userRatings = ratings.filter("userID=0")
    for rating in userRatings.collect():
        print (movieNames[rating['movieID']], rating['ratings'])

    print("\nTop 20 movie recommendations")

    ratingsCount = ratings.groupBy("movieID").count().filter("count > 100")
    # Create test dataFrames using movieID for userID '0'
    popularMovies = ratingsCount.select("movieID").withColumn('userID',lit(0))
    # Creating an ALS collaborative filtering model
    recommendations = model.transform(popularMovies)

    topTen = recommendations.sort(recommendations.prediction.desc()).take(10)

    for rating in topTen:
        print (movieNames[rating['movieID']], rating['prediction'])

    spark.stop()