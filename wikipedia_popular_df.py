import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wikipedia_popular_df').getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def path_to_hour(path):
    strs = re.compile('/').split(path)
    return strs[-1][11:22]

def main(inputs, output):
    
    wikipedia_schema = types.StructType([
    types.StructField('language', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('views', types.LongType(), True),
    types.StructField('bytes', types.LongType(), True)])
	
    data = spark.read.csv(inputs, sep=' ', schema = wikipedia_schema).withColumn('filename', functions.input_file_name())
    hours_function = functions.udf(path_to_hour, types.StringType())
    data = data.select('language', 'title', 'views', 'bytes', hours_function('filename').alias('hour'))
    data = data.where((data.language == 'en') & (data.title != 'Main_Page') & (~(data.title.startswith('Special:'))))
    data.cache()
    maxviews = data.groupby('hour').agg(functions.max('views').alias('views'))    
    
    maxviews = functions.broadcast(maxviews)	
    output_df = data.join(maxviews, ['hour','views'], 'inner').select(['hour', 'title', 'views']).orderBy('hour')
    output_df.write.json(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
