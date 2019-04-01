from pyspark.sql import *
def read():
    conf=SparkConf().setMaster("local[1]").setAppName("sql")
    sc=SparkContext(conf=conf)
    hive=HiveContext(sc)
    hive.sql('''
    SELECT  *  FROM kefu_first_log ORDER BY create_time DESC
    ''')
if __name__ == '__main__':
    read()