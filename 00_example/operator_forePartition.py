from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('foreachPartition')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1,10),3)

    def ride(data):
        result = list()
        for i in data:
            result.append(i)
        print(result)

    rdd.foreachPartition(ride)