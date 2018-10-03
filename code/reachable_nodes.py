from pyspark.sql import SparkSession
from pyspark.sql import functions
from graphframes.lib import AggregateMessages as AM
from graphframes import *
import sys


#arguments: "hdfs://ubuntu:9000/input/tiny-graph.txt" 0
#result should be 4

#get the vertex from nodes
def getVertexFrom(line, start):
    values = line.split(" ")
    if(values[1] == start):
        return (values[1], 1)
    else:
        return (values[1], 0)

#get the vertex to nodes
def getVertexTo(line, start):
    values = line.split(" ")
    if(values[2] == start):
        return (values[2], 1)
    else:
        return (values[2], 0)

#get the edges
def getEdges(line):
    values = line.split(" ")
    return (values[1], values[2], 1)


if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Need two arguments!")
    else:
        input_path = sys.argv[1]
        start_node = sys.argv[2]
        spark = SparkSession.builder.appName("Reachable Nodes")\
            .getOrCreate()
        sc = spark.sparkContext
        input_file = sc.textFile(input_path)
        vertex_rdd_one = input_file.map(lambda line:getVertexFrom(line, start_node))
        vertex_rdd_two = input_file.map(lambda line:getVertexTo(line, start_node))
        edges_rdd = input_file.map(lambda line:getEdges(line))
        input_data = vertex_rdd_one.intersection(vertex_rdd_two) #get the intersection nodeId
        vertices = spark.createDataFrame(input_data, ['id', 'distance'])#get the vertices dataframe
        edges = spark.createDataFrame(edges_rdd, ["src", "dst", 'weight']) #get the edges dataframe
        graph = GraphFrame(vertices, edges)#construct graph
        aggregation = None
        for i in range(vertices.count()): ##loop ,keep every node has change its state.
            aggregation = graph.aggregateMessages(
                    functions.max(AM.msg).alias("distance"),
                    sendToSrc=None,
                    sendToDst=AM.src["distance"])
            graph = GraphFrame(aggregation, edges)
        ##if the node not eqult to start node and distance not equal to 0, it means the start node can reach that node
        count = aggregation.filter("id != "+start_node+" and distance !=" + str(0)).count()
        print("The start node {0} could reach {1} nodes".format(start_node, count))