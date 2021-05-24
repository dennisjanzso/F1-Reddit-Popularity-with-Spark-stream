from pyspark.streaming import StreamingContext
from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SQLContext
import psutil
import sys

dictionary = {'hamilton': 'HAM', 'ham': 'HAM', 'lewis': 'HAM',
            'bottas': 'BOT', 'bot': 'BOT', 'valtteri': 'BOT',
            'verstappen': 'VER', 'ver': 'VER', 'max': 'VER',
            'perez': 'PER', 'per': 'PER', 'checo': 'PER',
            'leclerc': 'LEC', 'charles': 'LEC', 'lec': 'LEC',
            'sainz': 'SAI', 'carlos': 'SAI', 'sai': 'SAI',
            'norris': 'NOR', 'lando': 'NOR', 'nor': 'NOR',
            'ricciardo': 'RIC', 'daniel': 'RIC', 'ric': 'RIC',
            'sebastian': 'VET', 'seb': 'VET', 'vettel': 'VET', 'vet': 'VET',
            'lance': 'STR', 'stroll': 'STR', 'str': 'STR',
            'gasly': 'GAS', 'peirre': 'GAS', 'gas': 'GAS',
            'tsunoda': 'TSU', 'yuki': 'TSU', 'tsu': 'TSU',
            'alonso': 'ALO', 'fernando': 'ALO', 'nando': 'ALO', 'alo': 'ALO',
            'ocon': 'OCO', 'estaban': 'OCO', 'oco': 'OCO',
            'raikkonen': 'RAI', 'kimi': 'RAI', 'iceman': 'RAI', 'rai': 'RAI',
            'giovinazzi': 'GIO', 'antonio': 'GIO', 'gio': 'GIO',
            'russel': 'RUS', 'george': 'RUS', 'rus': 'RUS',
            'latifi': 'LAT', 'nicholas': 'LAT', 'lat': 'LAT',
            'schumacher': 'MSC', 'mick': 'MSC', 'msc': 'MSC',
            'mazapin': 'MAZ', 'nikita': 'MAZ', 'maz': 'MAZ'}

conf = (SparkConf()
         .setMaster("local[2]")
         .setAppName("F1App")
         .set("spark.executor.memory", "1g"))

sc = SparkContext(conf = conf)
sc.setLogLevel("FATAL")

ssc = StreamingContext(sc, 2)

ssc.checkpoint("checkpoint_F1App")

dataStream = ssc.socketTextStream("localhost",9009)

def aggregate_words_count(new_values, total_sum):
        	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
        if ('sqlContextSingletonInstance3' not in globals()):
            globals()['sqlContextSingletonInstance3'] = SQLContext(spark_context)
        return globals()['sqlContextSingletonInstance3']

def process_rdd(time, rdd):
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(driver=w[0], driver_count=w[1]))
        words_df = sql_context.createDataFrame(row_rdd)
        words_df.registerTempTable("drivers")
        words_counts_df = sql_context.sql("select driver, driver_count from drivers order by driver_count desc limit 20")
        words_counts_df.show()
    except Exception as e:
        print(e)
        er = sys.exc_info()[0]
        print("Error: %s" % er)

words = dataStream.flatMap(lambda line: line.split(" "))

pairs = words.filter(lambda word: word.lower() in dictionary).map(lambda name: (dictionary[name.lower()], 1))

words_totals = pairs.updateStateByKey(aggregate_words_count)

words_totals.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()