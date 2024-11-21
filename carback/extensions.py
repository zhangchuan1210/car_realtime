from pyspark.sql import SparkSession
from flask_sqlalchemy import SQLAlchemy
db = SQLAlchemy()
# Initialize Spark session
spark = SparkSession.builder.appName("CompanyApp")\
         .master("local[*]").config("spark.hadoop.fs.defaultFS", "file:///").config("spark.hadoop.io.file.buffer.size", "65536") \
         .getOrCreate()
