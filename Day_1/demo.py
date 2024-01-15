from pyspark import SparkContext, SparkConf
from pyspark import pandas as pd
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

config = SparkConf().setAppName("Spark Tutor").setMaster("local[2]")

sc = SparkContext(conf=config)
data = [1, 2, 3, 4, 5, 6]
rdd = sc.parallelize(data)
disFile = sc.textFile("dataset/data.txt")
disFile.foreach(lambda x: print(x))
length = disFile.map(lambda s: len(s))
length.foreach(lambda x: print(x))
total = length.reduce(lambda a,b: a+b)
print(total)