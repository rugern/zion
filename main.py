import os
import math
import pyspark
from pyspark.mllib.recommendation import ALS

sc = pyspark.SparkContext()
datasetFolder = 'ml-latest'


def parseData(raw, header, numberOfTokens):
    return raw \
            .filter(lambda line: line != header) \
            .map(lambda line: line.split(",")) \
            .map(lambda tokens: tuple(tokens[i] for i in range(numberOfTokens))) \
            .cache()


def extractData(filePath):
    dataRaw = sc.textFile(filePath)
    header = dataRaw.take(1)[0]
    data = parseData(dataRaw, header, 3)
    return dataRaw, header, data


def getRatingCountAndAverages(ratingsData):
    numberOfRatings = len(ratingsData[1])
    averageRating = sum(float(rating) for rating in ratingsData[1]) / numberOfRatings
    movieId = int(ratingsData[0])
    return movieId, (numberOfRatings, averageRating)


ratingsPath = os.path.join('datasets', datasetFolder, 'ratings.csv')
ratingsDataRaw, ratingsHeader, ratingsData = extractData(ratingsPath)

moviesPath = os.path.join('datasets', datasetFolder, 'movies.csv')
moviesDataRaw, moviesHeader, moviesData = extractData(moviesPath)
movieTitles = moviesData.map(lambda x: (int(x[0]), x[1]))

trainingRatingData, testRatingData = ratingsData.randomSplit([7, 3], seed=0)
predictRatingData = testRatingData.map(lambda x: (x[0], x[1]))

movieRatingMetadata = ratingsData \
        .map(lambda x: (x[1], x[2])) \
        .groupByKey() \
        .map(getRatingCountAndAverages)

newUserId = 0

# The format of each line is (userID, movieID, rating)
newUserRatingsRaw = [
     (0,260,9), # Star Wars (1977)
     (0,1,8), # Toy Story (1995)
     (0,16,7), # Casino (1995)
     (0,25,8), # Leaving Las Vegas (1995)
     (0,32,9), # Twelve Monkeys (a.k.a. 12 Monkeys) (1995)
     (0,335,4), # Flintstones, The (1994)
     (0,379,3), # Timecop (1994)
     (0,296,7), # Pulp Fiction (1994)
     (0,858,10) , # Godfather, The (1972)
     (0,50,8) # Usual Suspects, The (1995)
    ]
newUserRatings = sc.parallelize(newUserRatingsRaw)
ratingsData = ratingsData.union(newUserRatings)

newUserRatedMovies = map(lambda x: x[1], newUserRatingsRaw)
newUserUnratedMovies = moviesData \
        .filter(lambda x: x[0] not in newUserRatedMovies) \
        .map(lambda x: (newUserId, x[0]))


seed = 5
iterations = 10
regularizationParameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

minError = float('inf')
bestRank = -1
bestIteration = -1

rank = ranks[0]
model = ALS.train(ratingsData, rank, seed=seed, iterations=iterations, lambda_=regularizationParameter)
newUserPredictedRatings = model.predictAll(newUserUnratedMovies)
newUserRecommendations = newUserPredictedRatings \
        .map(lambda x: (x.product, x.rating)) \
        .join(movieTitles) \
        .join(movieRatingMetadata) \
        .map(lambda x: (x[1][0][1], x[1][0][0], x[1][1][0]))

topMovies = newUserRecommendations \
        .filter(lambda x: x[2] >= 25) \
        .takeOrdered(20, key=lambda x: -x[1])

print("Top recommendations:\n{}".format("\n".join(map(str, topMovies))))

# for rank in ranks:
#     model = ALS.train(trainingRatingData, rank, seed=seed, iterations=iterations, lambda_=regularizationParameter)
#     predictions = model.predictAll(predictRatingData) \
#             .map(lambda r: ((r[0], r[1]), r[2]))
#     ratesAndPrediction = testRatingData \
#             .map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))) \
#             .join(predictions)
#     error = math.sqrt(ratesAndPrediction.map(lambda r: (r[1][0] - r[1][1])**2).mean())
#     errors[err] = error
#     err += 1
#     print('For rank {} the RMSE is {}'.format(rank, error))
#     if error < minError:
#         minError = error
#         bestRank = rank
# 
# print('The best model was trained with rank {}'.format(bestRank))
