import sys
import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

def main():
    date = sys.argv[1]
    base_input_path = sys.argv[2]
    base_output_path = sys.argv[3]

    conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    # Напишите директорию чтения в общем виде
    events = sql.read.json(f'{base_input_path}/date={date}')

    # Напишите директорию записи
    events \
        .write \
        .partitionBy('event_type') \
        .format('parquet') \
        .mode('overwrite') \
        .save(f'{base_output_path}/date={date}')


if __name__ == "__main__":
    main()
