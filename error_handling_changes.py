from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local[*]").enableHiveSupport().getOrCreate()

us_county=spark.read.option("header",True).csv("/user/spark/covid/covid-data/enigma-nytimes-data-in-usa/csv/us_county/")

us_states=spark.read.option("header",True).csv("/user/spark/covid/covid-data/enigma-nytimes-data-in-usa/csv/us_states/")

states_daily=spark.read.option("header",True).csv("/user/spark/covid/covid-data/rearc-covid-19-testing-data/csv/states_daily")

us_total_latest=spark.read.option("header",True).csv("/user/spark/covid/covid-data/rearc-covid-19-testing-data/csv/us-total-latest")

us_daily=spark.read.option("header",True).csv("/user/spark/covid/covid-data/rearc-covid-19-testing-data/csv/us_daily")

hospital_beds=spark.read.option("header",True).json("/user/spark/covid/covid-data/rearc-usa-hospital-beds/usa-hospital-beds.geojson.4cCa297c")

CountyPopulation=spark.read.option("header",True).csv("/user/spark/covid/covid-data/static-datasets/csv/CountyPopulation")

countrycode=spark.read.option("header",True).csv("/user/spark/covid/covid-data/static-datasets/csv/countrycode")

stateabv=spark.read.option("header",True).csv("/user/spark/covid/covid-data/static-datasets/csv/state-abv")

us_county.createOrReplaceTempView("us_county")
hive_schema="`date1` String,`county` String,`state` String , `fips` int,`cases` int,`deaths` int"
hive_table_name="covid.us_county"
spark.sql(f"CREATE TABLE IF NOT EXISTS {hive_table_name} ({hive_schema}) STORED AS ORC")

us_states.createOrReplaceTempView("us_county")
hive_schema="`date1` String,`state` String , `fips` int,`cases` int,`deaths` int"
hive_table_name="covid.us_states"
spark.sql(f"CREATE  TABLE IF NOT EXISTS {hive_table_name} ({hive_schema}) STORED AS ORC")
us_states.printSchema()

hospital_beds.write.saveAsTable("covid.hospital_beds")
CountyPopulation.write.saveAsTable("covid.countyPopulation")
countrycode.write.saveAsTable("covid.countrycode")

stateabv.write.saveAsTable("covid.stateabv")

county_code=countrycode.withColumnRenamed("Alpha-2 code","Alpha_2").withColumnRenamed("Alpha-3 code","Alpha_3").withColumnRenamed("Numeric code","Numeric_code")
county_code.write.saveAsTable("covid.countrycode")
