from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType 
from pyspark.sql.types import IntegerType 
from pyspark.sql.functions import desc
import matplotlib.pyplot as plt 


def main(spark):
    fields=[StructField("Date", StringType(), True),\
            StructField("Time", StringType(), True),\
            StructField("TargetTemp", IntegerType(), True),\
            StructField("ActualTemp", IntegerType(), True),\
            StructField("System", IntegerType(), True),\
            StructField("SystemAge", IntegerType(), True),\
            StructField("BuildingId", IntegerType(), True)]
    schema = StructType(fields)
    hvacdf = spark.read.format("csv").option("header","true").schema(schema).load("c:\\sparksqlucs\\HVAC.csv")
    hvacdf.registerTempTable("HVAC")
    hvacWithFlag1 = spark.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from HVAC")
    hvacWithFlag1.registerTempTable("HVAC1")
    Buildingfields=[StructField("BuildingID", IntegerType(), True),\
                    StructField("BuildingMgr", StringType(), True),\
                    StructField("BuildingAge", IntegerType(), True),\
                    StructField("HVACproduct", StringType(), True),\
                    StructField("Country", StringType(), True)]
    Buildingschema = StructType(Buildingfields)
    Buildingdf = spark.read.format("csv").option("header","true").schema(Buildingschema).load("c:\\sparksqlucs\\building.csv")
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
    CountryLabel=countrieswithTempchangeOften.rdd.map(lambda x : x["country"]).collect()
    Count=countrieswithTempchangeOften.rdd.map(lambda x : x["count"]).collect()
    #creating a pie chart using matplot to visualize the final outcome (countries with frequent temperature  changes)
    explode = (0.3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0.3)
    plt.pie(Count, labels=CountryLabel,explode=explode,autopct= lambda p : '{:.2f}% \n({:d})'.format(p,int(round(p * sum(Count)/100))))
    plt.show()
    
if __name__ == "__main__" :
    spark = SparkSession.builder.appName("Temperature Change across world")\
    .getOrCreate()
    main(spark)
    