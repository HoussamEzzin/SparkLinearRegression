from pyspark import SparkContext as sc
from pyspark.sql import SparkSession


spark1 = SparkSession.builder.appName('SQL').getOrCreate()
df = spark1.read.csv('data/appl_stock.csv',inferSchema=True,header=True)

df.printSchema()


df.createOrReplaceTempView('stock')
result = spark1.sql("SELECT * FROM stock LIMIT 5")

print(result)

result.show()