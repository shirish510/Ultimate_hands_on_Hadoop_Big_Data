from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames={}
    with open("ml-100k/u.item") as f:
        for lines in f:
            fields = lines.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames

def parseInput(lines):
    fields = lines.split('\t')
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    conf = SparkConf().getAppName("MoviesLowestRatings")
    sc = SparkContext(conf=conf)

    movieNames=loadMovieNames()
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    ratings = lines.map(parseInput)

    ratingsTotalCount = ratings.reduceByKey(lambda movie1, movie2: movie1[0]+movie2[0], movie1[1]+movie2[1])
    ratingsLeast = ratingsTotalCount.filter(lambda x:x[1][1]>10)
    avgRatings = ratingsLeast.mapValues(lambda moviesAvg: moviesAvg[0]/moviesAvg[1])

    sortRatings = avgRatings.sortBy(lambda x: x[1])

    topTen = sortRatings.take(10)

    for movie in topTen:
        print (movieNames[movie[0]], movie[1])

    spark.stop()

