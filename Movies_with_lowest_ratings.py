from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    movieNames = {}
    with open ("ml-100k/u.item") as f:
        for lines in f:
            fields = lines.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames

def parseInput(lines):
    fields = lines.split('\t')
    return Row(movieID=int(fields[1]), ratings=float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Lowestmovies").getOrCreate()
    movieNames = loadMovieNames()
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    rating = lines.map(parseInput)
    ratingsDataFrame = spark.createDataFrame(rating)
    #counts = ratingsDataFrame.groupBy("movieID").count().filter("count>10")
    movieAvg = ratingsDataFrame.groupBy("movieID").avg("ratings")
    counts = ratingsDataFrame.groupBy("movieID").count()
    moviesAvgCount = counts.join(movieAvg, "movieID")
    leastPopular = moviesAvgCount.filter("count>=10")

    topTen = leastPopular.orderBy("avg(ratings)").take(10)

    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    spark.stop()