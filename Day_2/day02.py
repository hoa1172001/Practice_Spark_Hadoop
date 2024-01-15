from pyspark import SparkContext, SparkConf
from pyspark import pandas as pd
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

config = SparkConf().setAppName("Spark Tutor").setMaster("local[2]")

sc = SparkContext(conf=config)

rdd = sc.textFile("G:/demoSpark/Day_2/data/test.txt")
rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
print(rdd5.collect())