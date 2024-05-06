from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from operator import add
import re
import os
os.environ['HADOOP_CONF_DIR'] = "/export/server/hadoop/etc/hadoop"

if __name__ == '__main__':
    conf = SparkConf().setAppName('accumulator').setMaster("yarn")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile('hdfs://node1:8020/input/accumulator_broadcast_data.txt')
    abnormal_char = [',','.','!','#','$','%']
    broadcast = sc.broadcast(abnormal_char)
    acmlt = sc.accumulator(0)

    lines_rdd = file_rdd.filter(lambda line:line.strip())
    data_rdd = lines_rdd.map(lambda x:x.strip())

    words_rdd = data_rdd.flatMap(lambda x: re.split('\s+', x))

    def filter_func(data):
        global acmlt
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            acmlt += 1
            return  False
        else:
            return True

    normal_word_rdd = words_rdd.filter(filter_func)
    result_rdd = normal_word_rdd.map(lambda  x:(x,1)).reduceByKey(add)
    print(result_rdd.collect())
    print(acmlt)