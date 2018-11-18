import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather temp range').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def main(inputs, output):

    observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),])

    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.createOrReplaceTempView("weather_df")
    sqldf = spark.sql("SELECT * FROM weather_df WHERE qflag IS NULL AND (observation = 'TMAX' OR observation = 'TMIN')")
    sqldf.createOrReplaceTempView("weather_df")
    sqldf = spark.sql("SELECT *, IF(observation = 'TMAX', value/10, NULL) AS tmax FROM weather_df")
    sqldf.createOrReplaceTempView("weather_df")
    sqldf = spark.sql("SELECT *, IF(observation = 'TMIN', value/10, NULL) AS tmin FROM weather_df")
    sqldf.createOrReplaceTempView("weather_df")
    tmax = spark.sql("SELECT station, date, tmax FROM weather_df")
    tmax.createOrReplaceTempView("tmax_df")
    tmin = spark.sql("SELECT station, date, tmin FROM weather_df")
    tmin.createOrReplaceTempView("tmin_df")
    
    joined = spark.sql("SELECT tmax_df.date, tmax_df.station, tmax_df.tmax, tmin_df.tmin FROM tmax_df INNER JOIN tmin_df ON tmax_df.station = tmin_df.station AND tmax_df.date = tmin_df.date")
    joined.createOrReplaceTempView("joined_df")
    joined = spark.sql("SELECT *, ROUND(tmax-tmin,1) AS range FROM joined_df")
    joined.createOrReplaceTempView("joined_df")
    joined = spark.sql("SELECT date, station, range FROM joined_df WHERE range IS NOT NULL")
    joined.createOrReplaceTempView("joined_df")
    joined = spark.sql("SELECT date, station, range, MAX(range) OVER(PARTITION BY date) as maxrange FROM joined_df")
    joined.createOrReplaceTempView("joined_df")
    output_df = spark.sql("SELECT date, station, range FROM joined_df WHERE range = maxrange ORDER BY date")
    
    output_df.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
 