from pyspark.sql import SparkSession
import os
import sys

#arguments: "hdfs://ubuntu:9000/input/tiny-graph.txt" "hdfs://ubuntu:9000/output/"


#get the initiate state of each node,<nodeId, <length, 1>>
def getNodesPair(line):
    values = line.split(" ")
    return (values[1], (float(values[3]), 1))

#get <id, <sum, count>>
def getSum(nodeOne, nodeTwo):
    return (nodeOne[0]+nodeTwo[0], nodeOne[1]+nodeTwo[1])


if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Need two arguments!")
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        spark = SparkSession.builder.appName("Average Length").getOrCreate()
        sc = spark.sparkContext
        input_file = sc.textFile(input_path)
        nodes = input_file.map(lambda line: getNodesPair(line))
        sumPair = nodes.reduceByKey(lambda nodeOne, nodeTwo: getSum(nodeOne, nodeTwo))
        averagePair = sumPair.map(lambda keyValue: (keyValue[0], keyValue[1][0] /keyValue[1][1]))#get <ID, average>
        result = averagePair.map(lambda keyValue: (keyValue[1], keyValue[0])).sortByKey(False)\
            .map(lambda keyValue: (str(keyValue[1])+"\t"+ str(keyValue[0])))#format
        result.saveAsTextFile(output_path)



        
    