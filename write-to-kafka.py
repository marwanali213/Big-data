from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = (
    StructType()
    .add("iso_code", StringType())
    .add("continent", StringType())
    .add("location", StringType())
    .add("date", StringType())
    .add("total_cases", StringType())
    .add("new_cases", StringType())
    .add("new_cases_smoothed", StringType())
    .add("total_deaths", StringType())
    .add("new_deaths", StringType())
    .add("new_deaths_smoothed", StringType())
    .add("total_cases_per_million", StringType())
    .add("new_cases_per_million", StringType())
    .add("new_cases_smoothed_per_million", StringType())
    .add("total_deaths_per_million", StringType())
    .add("new_deaths_per_million", StringType())
    .add("new_deaths_smoothed_per_million", StringType())
    .add("reproduction_rate", StringType())
    .add("icu_patients", StringType())
    .add("icu_patients_per_million", StringType())
    .add("hosp_patients", StringType())
    .add("hosp_patients_per_million", StringType())
    .add("weekly_icu_admissions", StringType())
    .add("weekly_icu_admissions_per_million", StringType())
    .add("weekly_hosp_admissions", StringType())
    .add("weekly_hosp_admissions_per_million", StringType())
    .add("total_tests", StringType())
    .add("new_tests", StringType())
    .add("total_tests_per_thousand", StringType())
    .add("new_tests_per_thousand", StringType())
    .add("new_tests_smoothed", StringType())
    .add("new_tests_smoothed_per_thousand", StringType())
    .add("positive_rate", StringType())
    .add("tests_per_case", StringType())
    .add("tests_units", StringType())
    .add("total_vaccinations", StringType())
    .add("people_vaccinated", StringType())
    .add("people_fully_vaccinated", StringType())
    .add("total_boosters", StringType())
    .add("new_vaccinations", StringType())
    .add("new_vaccinations_smoothed", StringType())
    .add("total_vaccinations_per_hundred", StringType())
    .add("people_vaccinated_per_hundred", StringType())
    .add("people_fully_vaccinated_per_hundred", StringType())
    .add("total_boosters_per_hundred", StringType())
    .add("new_vaccinations_smoothed_per_million", StringType())
    .add("new_people_vaccinated_smoothed", StringType())
    .add("new_people_vaccinated_smoothed_per_hundred", StringType())
    .add("stringency_index", StringType())
    .add("population_density", StringType())
    .add("median_age", StringType())
    .add("aged_65_older", StringType())
    .add("aged_70_older", StringType())
    .add("gdp_per_capita", StringType())
    .add("extreme_poverty", StringType())
    .add("cardiovasc_death_rate", StringType())
    .add("diabetes_prevalence", StringType())
    .add("female_smokers", StringType())
    .add("male_smokers", StringType())
    .add("handwashing_facilities", StringType())
    .add("hospital_beds_per_thousand", StringType())
    .add("life_expectancy", StringType())
    .add("human_development_index", StringType())
    .add("population", StringType())
    .add("excess_mortality_cumulative_absolute", StringType())
    .add("excess_mortality_cumulative", StringType())
    .add("excess_mortality", StringType())
    .add("excess_mortality_cumulative_per_million", StringType())
)
# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", "D:\pyspark projects\stet\data") \
    .load() \

# Select specific columns from "data"
#df = streaming_df.select("name", "age")

#df = streaming_df.select(col("name").alias("key"), to_json(col("age")).alias("value"))
df = streaming_df.select(to_json(struct("*")).alias("value"))

# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "dull") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
