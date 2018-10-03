from pyspark.sql import SparkSession
import sys

#arguments: "hdfs://ubuntu:9000/input/tiny-data.txt" "hdfs://ubuntu:9000/output/"

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Need two arguments!")
    else:
        spark = SparkSession.builder.appName("wordcount").getOrCreate()
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        sc = spark.sparkContext
        textFile = sc.textFile(input_path)
        words = textFile.flatMap(lambda line: line.split(" "))
        pairs =  words.map(lambda word: (word, 1))
        frequencies= pairs.reduceByKey(lambda a, b: a + b)
        print(frequencies.collect())
        spark.stop()