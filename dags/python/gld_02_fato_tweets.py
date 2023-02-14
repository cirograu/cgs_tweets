from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as Func

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Cria contexto Spark
spark = SparkSession.builder \
    .master('local') \
    .appName('TwitterApp') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .getOrCreate()

contexto = spark.sparkContext
sqlContext = SQLContext(contexto)

df = sqlContext.read.parquet('/usr/local/spark/resources/data/svr/parquet/tweets.parquet')
print('lido')
df2 = df.withColumn('data', Func.to_timestamp(Func.substring('date', 1 , 10),"yyyy-MM-dd")) \
        .withColumn("retweet",
                          Func.when(df.retweetCount > 0,'RETWEET').
                           otherwise('NORMAL')
                          )

#df2.printSchema()

#############################################
# Gera Fato
#############################################

df2.createOrReplaceTempView("tweets_v");
sql = "SELECT data, hashtaginformada, \
        username, \
        sourceLabel, \
        texto , \
        location , \
        retweetCount, \
        likeCount, \
        1 tweets,  \
        retweet \
       FROM tweets_v"

df_fato = spark.sql(sql);

#Gravar parquet - sempre overwrite
df_fato.write.mode('overwrite').parquet('/usr/local/spark/resources/data/gld/parquet/fato_tweets.parquet')
print('======================================')
print('Gold fato_tweets.parquet gerado')
print('======================================')