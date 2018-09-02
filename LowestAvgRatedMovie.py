from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def loadMovieNames():
        movieNames={}
        with open("ml-100k/u.item") as f:
        for line in f:
                fields = line.split('|')
                movieNames[int(fields[0])] = fields[1]
        return movieNames

def parseInput(line):
        fields = line.split('\t')
        return Row(movieID = int(fields[1]), ratings = float(fields[2]))

if __name__ == "__main__":
        # Create Spark session:
        spark = SparkSession.builder.appName("LowestAvgRated").getOrCreate()

        # Load the movie names
        movieNames = loadMovieNames()
        # Load the movie ratings data
        lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
        # Map the data to (movieID, ratings)
        movies = lines.map(parseInput)
        # Reduce it to (movieID, avg)

        moviesDataset = spark.createDataFrame(movies)

        averageRatings = moviesDataset.groupBy("movieID").avg("ratings")

        counts = moviesDataset.groupBy("movieID").count()

        moviesavgandcount = counts.join(averageRatings,"movieID")

        topTen = moviesavgandcount.orderBy("avg(ratings").take(10)

        for movie in topTen:
            print (movieNames[movie[0]], movie[1], movie[2])

        spark.stop()
