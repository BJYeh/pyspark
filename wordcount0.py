import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1))
    output = counts.collect()
    print("Mapper")
    for (word, count) in output:
        print("%s: %i" % (word, count))
    
    counts0 = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .sortByKey() \

    output0 = counts0.collect()
    print("Shuffle Sort")
    for (word, count) in output0:
        print("%s: %i" % (word, count))
        
    counts1 = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output1 = counts1.collect()
    print("Reduce -Sort")      
    for (word, count) in output1:
        print("%s: %i" % (word, count))
        
   

    spark.stop()
