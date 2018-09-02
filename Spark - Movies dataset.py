from pyspark import SparkConf, SparkContext

def loadMovies():
    movieNames = {}
    with open ("/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames

def parseInput(line):
    fields = line.split('\t')
    return (int(fields[1]), (float(fields[2]),1.0))

if __name__ == "__main__":
    # The main script - Create SparkContext
    conf = SparkConf().setappname("Worstmovies")
    sc = SparkContext(conf=conf)

    # Load up movienames based on movieID
    movieNames = loadMovies()

    # Laod up the raw DataFile u.data
    lines = sc.TextFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (ratings, 1.0))
    ratingsData = lines.map(parseInput)

    # Reduce to (movieID, (sum of ratings, total number of ratings))
    ratingsTotalAndCount = ratingsData.reduceByKey(lambda movie1, movie2: (movie1[0]+movie2[0],movie1[1]+movie2[1]))

    # Reduce to (movieID, avg of ratings)
    ratingsAverage = ratingsTotalAndCount.mapValues(lambda totalcount: totalcount[0]/totalcount[1])

    # Sort by average - x[1] - because the second column has average
    sortedMovies = ratingsAverage.sortBy(lambda x: x[1])

    results = sortedMovies.take(10)

    for result in results:
        print movieNames[result[0]], results[1]
