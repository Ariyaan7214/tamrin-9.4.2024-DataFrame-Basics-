# DataFrame Basics
(DataFrame Basics) tamrin 9.4.2024 github
!pip install pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Basics").getOrCreate()
from google.colab import drive
drive.mount('/content/drive')
df = spark.read.json('drive/MyDrive/people.json')
# Showing the data
df.show()
df.printSchema()
df.columns
df.describe().show()
df.summary()
from pyspark.sql.types import StructField,StringType,IntegerType,StructType
data_schema = [StructField("age", IntegerType(), True),StructField("name", StringType(), True)]
data_schema
final_struc = StructType(fields=data_schema)
final_struc
df = spark.read.json('drive/MyDrive/people.json', schema=final_struc)
df.printSchema()
df.show()
type(df['age'])
df['age'].show()
df.select('age')
type(df.select('age'))
df.select('age').show()
# Returns list of Row objects
df.head(2)
df.select(['age','name'])
df.select(['age','name']).show()
# Adding a new column
df.withColumn('newage',df['age']+2).show()
df.show()
# Simple Rename
df.withColumnRenamed('age','supernewage').show()
adf=df.withColumn('doubleage',df['age']*2)
adf.show()
df.withColumn('add_one_age',df['age']+1).show()
df.withColumn('half_age',df['age']/2).show()
df.withColumn('half_age',df['age']/2)
# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")
sql_results = spark.sql("SELECT * FROM people")
sql_results
sql_results.show()
spark.sql("SELECT * FROM people WHERE age=30").show()
