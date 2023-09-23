"""
Author: Cody Viscardis
Last Updated: 09/22/2023
File Explanation:
    PySpark file which pulls King County housing data from a csv file
    and analyzes it through various methods
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("KingCountyHousingData").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Dataset sourced from kaggle
houses = spark.read.option("header", "true").option("inferSchema", "true")\
         .csv("file:///SparkCourse/kc_house_data.csv")
        # Change filepath to location of your dataset
        
# Changed date column from string datatype to date
houses = houses.withColumn('date', func.to_date('date'))

print("Inferred schema:")
houses.printSchema()

# Clean up inconsistent data where bedrooms can be 0
# and where houses with more than 8 bedrooms are cheaper than 8
houses = houses.filter( (houses.bedrooms != 0) & (houses.bedrooms < 9) )

print("Homes per ZIP code:")
houses.groupBy("zipcode").count().show()

print("Average house price for King County ZIP codes:")
houses.groupBy("zipcode").agg(func.round(func.avg("price"), 2)\
                              .alias("average_price")).orderBy("zipcode").show()

print("Average price per number of bedrooms")
houses.groupBy("bedrooms").agg(func.round(func.avg("price"), 2)\
                               .alias("average_price")).orderBy("bedrooms").show()



spark.stop()
