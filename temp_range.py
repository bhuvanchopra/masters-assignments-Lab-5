import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import Window

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather temp range').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def tmax_udf(x,y):
    if x == 'TMAX':
        return y/10
    else:
        return None

def tmin_udf(x,y):
    if x == 'TMIN':
        return y/10
    else:
        return None

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

    tmax_udfunc = functions.udf(tmax_udf, types.FloatType())
    tmin_udfunc = functions.udf(tmin_udf, types.FloatType())
    weather = spark.read.csv(inputs, schema=observation_schema)
    weather = weather.filter(weather.qflag.isNull())
    weather = weather.filter((weather.observation == 'TMAX') | (weather.observation == 'TMIN'))
    weather.cache()
    tmax = weather.withColumn('tmax', tmax_udfunc(weather.observation,weather.value))
    tmin = weather.withColumn('tmin', tmin_udfunc(weather.observation,weather.value))
    tmax = tmax['station','date','tmax']
    tmin = tmin['station','date','tmin']
    joined = tmax.join(tmin, ['station','date'])
    joined = joined.withColumn('range', functions.round(joined.tmax - joined.tmin,1))
    joined = joined.filter(joined.range.isNotNull())
    joined.cache()
    weather.unpersist()
    ranged = joined['station','date','range'].orderBy('date')
    #In the below step, I wanted to use the window function instead of the GroupBy method for grouping.
    w = Window.partitionBy('date')
    maxxed = ranged.withColumn('maxrange', functions.max('range').over(w)).where(functions.col('range') == functions.col('maxrange')).drop('maxrange')

    maxxed['date','station','range'].write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
