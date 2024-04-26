from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('countbykey')
    sc = SparkContext(conf=conf)

    rdd1 = sc.textFile('../data/words.txt')
    rdd2 = rdd1.flatMap(lambda x:x.split(' '))
    rdd3 = rdd2.map(lambda x:(x, 1))
    result = rdd3.countByKey()

    print(result)