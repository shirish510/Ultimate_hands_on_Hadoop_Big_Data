from starbase import Connection

c = Connection("127.0.0.1", "8000")

ratings = c.table('ratings')

if (ratings.exists()):
    ratings.drop()

ratings.create('rating')

batch = ratings.batch()

ratingsFile = open("C:\\Users\Lanish Alvino Evin\Documents\Datasets\ml-100k\u.data", "r")

for line in ratingsFile:
    (userID, movieID, rating, timeStamp) = line.split('\t')
    batch.update(userID, {'rating': {movieID: rating}})

ratingsFile.close()

batch.commit(finalize=True)

print(ratings.fetch("1"))
print(ratings.fetch("33"))