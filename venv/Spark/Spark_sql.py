from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
def read():
    conf=SparkConf().setAppName("sql")
    sc=SparkContext(conf=conf)
    hive=HiveContext(sc)
    hive.sql('''
    SELECT  *  FROM kefu_first_log ORDER BY create_time DESC
    ''').show
def write():
    return
if __name__ == '__main__':
    read()