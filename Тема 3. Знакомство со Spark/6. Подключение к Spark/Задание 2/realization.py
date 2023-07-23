from pyspark.sql import SparkSession

# Подключение с выбором режима работы (master)
spark = SparkSession.builder \
    .master('yarn') \
    .appName('My second session') \
    .getOrCreate()
