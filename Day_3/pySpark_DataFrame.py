from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()
#Create empty dataFrame with Schema
schema = StructType([
    StructField('firstname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lastname', StringType(), True)
    ])
emptyRDD = spark.sparkContext.emptyRDD()
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()

#Convert Empty RDD to DataFrame
df1 = emptyRDD.toDF(schema)
df1.printSchema()

#Convert rdd to dataframe
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
df2 = rdd.toDF()
df2.printSchema()
df2.show(truncate= False) #print value of dept

#load data from one file text to rdd, use function map() and convert to dataframe
rdd = spark.sparkContext.textFile("G:/demoSpark/Day_2/data/test.txt")
rdd2 = rdd.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
df3 = rdd5.toDF()
df3.printSchema()
df3.show(truncate= False)

# Nested StructType

structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
    ]
structureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
        ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
    ])

df4 = spark.createDataFrame(data=structureData,schema=structureSchema)
df4.printSchema()
df4.show(truncate=False)