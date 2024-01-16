import pyspark
import os
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import col, struct, when
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[1]").appName('SparkDemo.com').getOrCreate()
colObj = lit("SparkDemo.com")

data=[("James",23),("Ann",40)]
df=spark.createDataFrame(data).toDF("name.fname","gender")
df.printSchema()
# Using DataFrame object (df)
df.select(df.gender).show()
df.select(df["gender"]).show()
#Accessing column name with dot (with backticks)
df.select(df["`name.fname`"]).show()
#Using SQL col() function
df.select(col("gender")).show()
#Accessing column name with dot (with backticks)
df.select(col("`name.fname`")).show()

#Create DataFrame with struct using Row class
data=[Row(name="James",prop=Row(hair="black",eye="blue")),
      Row(name="Ann",prop=Row(hair="grey",eye="black"))]
df1=spark.createDataFrame(data)
df1.printSchema()
#Access struct column
df1.select(df1.prop.hair).show()
df1.select(df1["prop.hair"]).show()
df1.select(col("prop.hair")).show()
#Access all columns from struct
df1.select(col("prop.*")).show()
#PySpark Column Operators
data=[(100,2,1),(200,3,4),(300,4,4)]
df2=spark.createDataFrame(data).toDF("col1","col2","col3")

#Arthmetic operations
df2.select(df2.col1 + df2.col2).show()
df2.select(df2.col1 - df2.col2).show()
df2.select(df2.col1 * df2.col2).show()
df2.select(df2.col1 / df2.col2).show()
df2.select(df2.col1 % df2.col2).show()

df2.select(df2.col2 > df2.col3).show()
df2.select(df2.col2 < df2.col3).show()
df2.select(df2.col2 == df2.col3).show()

