from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("LinearRegression").getOrCreate()


data = spark.read.csv('Student_Grades_Data.csv', header=True,inferSchema=True)


data.printSchema()

data.show()


feature_cols = data.columns[:-1]

