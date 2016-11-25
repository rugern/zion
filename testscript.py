import pyspark

sc = pyspark.SparkContext()
print(sc.parallelize(range(1000)).count())
