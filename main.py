import os
import math
import pyspark
from pyspark.mllib.recommendation import ALS

sc = pyspark.SparkContext()


def extractData(raw, header, numberOfTokens):
    return raw \
            .filter(lambda line: line != header) \
            .map(lambda line: line.split(",")) \
            .map(lambda tokens: tuple(tokens[i] for i in range(numberOfTokens))) \
            .cache()


ratingsPath = os.path.join('datasets', 'ml-latest-small', 'ratings.csv')
ratingsDataRaw = sc.textFile(ratingsPath)
ratingsHeader = ratingsDataRaw.take(1)[0]
ratingsData = extractData(ratingsDataRaw, ratingsHeader, 3)

moviesPath = os.path.join('datasets', 'ml-latest-small', 'movies.csv')
moviesDataRaw = sc.textFile(moviesPath)
moviesHeader = moviesDataRaw.take(1)[0]
moviesData = extractData(moviesDataRaw, moviesHeader, 3)

trainingRDD, testRDD = ratingsData.randomSplit([7, 3], seed=0)
testForPredictRDD = testRDD.map(lambda x: (x[0], x[1]))

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
for rank in ranks:
    model = ALS.train(trainingRDD, rank, seed=seed, iterations=iterations, lambda_=regularizationParameter)
    predictions = model.predictAll(testForPredictRDD) \
            .map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPrediction = testRDD \
            .map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))) \
            .join(predictions)
    error = math.sqrt(ratesAndPrediction.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    errors[err] = error
    err += 1
    print('For rank {} the RMSE is {}'.format(rank, error))
    if error < minError:
        minError = error
        bestRank = rank

print('The best model was trained with rank {}'.format(bestRank))
