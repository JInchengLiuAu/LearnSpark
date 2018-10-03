from pyspark.sql import SparkSession
import sys
import math
from secondary_sort import MySecondarySort

#argument hdfs://ubuntu:9000/input/tiny-data.txt hdfs://ubuntu:9000/output/ 0.5

#cacluate Jaccard Similarity
def getJaccardSim(intersection, union):
    return intersection/union

# get the prefix length, using formula: p = |r| - |r|*t + 1
def getPrefixLength(recordSize, threshold):
    return recordSize - math.ceil(recordSize*threshold)+1

#get format of seq(i), (RID, seq)),
def getPrefixPairValue(line, threshold):
    values = line.split(" ")
    rid = values[0]
    tokens = sorted(values[1:-1])
    prefixLength = getPrefixLength(len(tokens), threshold)
    #print("rid={}, length={}, len={}, valueslen={}, values={}".format(rid, prefixLength,len(tokens),len(values), values[1:-1]))
    for i in range(prefixLength):
        yield(tokens[i], {rid:tokens})

#compare Similarity
def compareSim(records, threshold):#seq(i), map(RID, seq))
    result_map = records[1]
    for i in result_map.keys():
        for j in result_map.keys():
            if i != j :
                set_one = set(result_map[i])
                set_two = set(result_map[j])
                inter = set_one.intersection(set_two)#get the intersection
                union = set_one.union(set_two)#get the union
                sim = getJaccardSim(len(inter), len(union))
                if(sim >= threshold):
                    pair = []
                    if (i<j):
                        pair = (i, j, sim)
                    else:
                        pair = (j, i, sim)
                    yield(pair)
    

if __name__ == "__main__":
    if(len(sys.argv) != 4):
        print("Need three arguments!")
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        threshold = sys.argv[3]
        spark = SparkSession.builder.appName("Set Sim Join").getOrCreate()
        sc = spark.sparkContext
        sc.addPyFile("secondary_sort.py")
        original_file = sc.textFile(input_path)#read the file

        input_file =   original_file.flatMap(lambda line: line.split("\n")) 
        ##step one get the token frequency,every line behind has one whiteplace
        tokens_frequency = input_file.flatMap(lambda line: line.split(" ")[1:-1])\
                            .map(lambda token: (token, 1))\
                            .reduceByKey(lambda a,b : a+b)\
                            .sortBy(lambda values: values[1]).map(lambda values: values[0])


        #step two ,seq(i), map(RID, seq))
        prefixes_rdd = input_file.flatMap(lambda line: getPrefixPairValue(line,float(threshold)))\
                            .reduceByKey(lambda map_one,map_two:{**map_one, **map_two})#add two map
                                
        result = prefixes_rdd.flatMap(lambda record:compareSim(record, float(threshold)))

        #Step three remove dupliate and format
        final_result = result.distinct().map(lambda line: (MySecondarySort(int(line[0]), int(line[1])), line[2])).sortByKey()\
                            .map(lambda record:"("+str(record[0].recordOne)+","+str(record[0].recordTwo)+")\t"+ str(record[1]))

        final_result.saveAsTextFile(output_path)
        spark.stop()
