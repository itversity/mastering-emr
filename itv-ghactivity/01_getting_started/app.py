from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    master('local'). \
    appName('GitHub Activity - Getting Started'). \
    getOrCreate()

spark.sql('SELECT current_date').show()
