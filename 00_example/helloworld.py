from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('Wordcounthelloworld')
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile(('hdfs://node1:8020/input/wordcount/words.txt'))

    word_rdd = file_rdd.flatMap(lambda line:line.split(' '))
    word_with_one_rdd = word_rdd.map(lambda x:(x, 1))
    result_rdd = word_with_one_rdd.reduceByKey(lambda a,b:a+b)

    print(result_rdd.collect())