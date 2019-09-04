from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType 
from pyspark.sql.types import IntegerType 
from pyspark.sql.functions import desc 

def main(spark):
    sc = spark.sparkContext
    data = sc.textFile("C:/sparksqlucs/HVAC.csv")
    header = data.first()
    dataWithoutHeader = data.filter(lambda row : row != header)
    fields=[StructField("Date", StringType(), True),\
            StructField("Time", StringType(), True),\
            StructField("TargetTemp", IntegerType(), True),\
            StructField("ActualTemp", IntegerType(), True),\
            StructField("System", IntegerType(), True),\
            StructField("SystemAge", IntegerType(), True),\
            StructField("BuildingId", IntegerType(), True)]
    hvac = dataWithoutHeader.map(lambda x : x.split(",")).map(lambda p: (p[0], p[1],int(p[2]),int(p[3]),int(p[4]),int(p[5]),int(p[6])))
    schema = StructType(fields)
    hvacdf = spark.createDataFrame(hvac,schema)
    hvacdf.registerTempTable("HVAC")
    hvacWithFlag1 = spark.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC")
    hvacWithFlag1.registerTempTable("HVAC1")
    data2 = sc.textFile("C:/sparksqlucs/building.csv")
    header1 = data2.first()
    data3 = data2.filter(lambda row : row != header1)
    Buildingfields=[StructField("BuildingID", IntegerType(), True),\
                    StructField("BuildingMgr", StringType(), True),\
                    StructField("BuildingAge", IntegerType(), True),\
                    StructField("HVACproduct", StringType(), True),\
                    StructField("Country", StringType(), True)]
    building = data3.map(lambda x : x.split(",")).map(lambda p: (int(p[0]), p[1],int(p[2]),p[3],p[4]))
    Buildingschema = StructType(Buildingfields)
    Buildingdf = spark.createDataFrame(building,Buildingschema)
    Buildingdf.registerTempTable("building")
    buildingWithHvac = spark.sql("select h.*, b.country, b.hvacproduct from building b join hvac1 h on b.BuildingId = h.Buildingid")
    buildingWithHvac.show()
    buildingWithHvac.registerTempTable("buildingWithHvacDetails")
    countryWithTemparatureFlag= buildingWithHvac.filter(buildingWithHvac["tempchange"]=="1")
    countrieswithTempchange=countryWithTemparatureFlag.groupBy("country").count()
    countrieswithTempchangeOften=countrieswithTempchange.sort(desc("count"))
    countrieswithTempchangeOften.show()
    Top2SystemAgeCountryWise=spark.sql("select  country, SystemAge FROM \
    ( SELECT country,SystemAge,dense_rank() OVER \
    (PARTITION BY country ORDER BY SystemAge DESC) as rank \
    FROM buildingWithHvacDetails ) tmp WHERE rank <= 2 ")
    Top2SystemAgeCountryWise.distinct().show()
    Lowest2SystemAgeCountryWise=spark.sql("select  country, SystemAge FROM \
    ( SELECT country,SystemAge,dense_rank() OVER \
    (PARTITION BY country ORDER BY SystemAge asc) as rank \
    FROM buildingWithHvacDetails ) tmp WHERE rank <= 2 ")
    Lowest2SystemAgeCountryWise.distinct().show()
if __name__ == "__main__" :
    spark = SparkSession.builder.appName("Temperature Change across world")\
    .getOrCreate()
    main(spark)
    
    