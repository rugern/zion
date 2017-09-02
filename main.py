import os
import math
import pyspark
from pyspark.mllib.recommendation import ALS

sc = pyspark.SparkContext()

# TODO: Bruk trainImplicit
# TODO: Legg til content-based, popular, editorial filtering (i framtiden)
# TODO: Artikkelanbefaling til bruker, og artikkelanbefaling basert på artikkel


# data = sc.parallelize(
# ratingsPath = os.path.join('datasets', 'export_06012017T1250_glomdalen.csv')
# ratingsDataRaw = sc.textFile(ratingsPath)
# ratingsHeader = ratingsDataRaw.take(1)[0]
# ratingsData = extractData(ratingsDataRaw, ratingsHeader)

# print(ratingsHeader)
# print(ratingsData.take(10))

# trainingRDD, testRDD = ratingsData.randomSplit([7, 3], seed=0)
# testForPredictRDD = testRDD.map(lambda x: (x[0], x[2]))
# 
# seed = 5
# iterations = 10
# regularizationParameter = 0.1
# ranks = [4, 8, 12]
# errors = [0, 0, 0]
# err = 0
# tolerance = 0.02
# 
# minError = float('inf')
# bestRank = -1
# bestIteration = -1
# for rank in ranks:
#     model = ALS.train(trainingRDD, rank, seed=seed, iterations=iterations, lambda_=regularizationParameter)
#     predictions = model.predictAll(testForPredictRDD) \
#         .map(lambda r: ((r[0], r[1]), r[2]))
#     ratesAndPrediction = testRDD \
#         .map(lambda r: ((r[0], r[1]), float(r[2]))) \
#         .join(predictions)
#     error = math.sqrt(ratesAndPrediction.map(lambda r: (r[1][0] - r[1][1])**2).mean())
#     errors[err] = error
#     err += 1
#     print('For rank {} the RMSE is {}'.format(rank, error))
#     if error < minError:
#         minError = error
#         bestRank = rank
# 
# print('The best model was trained with rank {}'.format(bestRank))
