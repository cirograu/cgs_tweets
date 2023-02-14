from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as Func
from pyspark.sql.functions import lit

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


df_brz = sqlContext.read.parquet('/usr/local/spark/resources/data/brz/parquet/tweets.parquet')
print('Bronze lido')

df_2 =  df_brz.withColumn("retweetCount",Func.col("retweetCount").cast("int")) \
          .withColumn("likeCount",Func.col("likeCount").cast("int")) 

try:
    df_svr = sqlContext.read.parquet('/usr/local/spark/resources/data/svr/parquet/tweets.parquet')
    print('Silver lido')
except:
    #Gravar parquet - sempre overwrite
    df_2.write.mode('overwrite').parquet('/usr/local/spark/resourcesdata/svr/parquet/tweets.parquet')
    df_2.write.partitionBy("hashtaginformada","ano","mes","dia").mode("append").parquet('/usr/local/spark/resources/data/svr/parquet/tweets.parquet')
    print('bronze tweets.parquet gerado')
    
v_qtd_tweets = df_brz.count()

distinct_df = df_2.distinct()

v_qtd_tweets_distincts = distinct_df.count()

print (f'Qtd registros: {v_qtd_tweets}')
print (f'Qtd registros distintos: {v_qtd_tweets_distincts}')

if v_qtd_tweets > v_qtd_tweets_distincts:
    #Gravar parquet - sempre overwrite
    distinct_df.write.mode('overwrite').parquet('/usr/local/spark/resources/data/svr/parquet/tweets.parquet')
    distinct_df.write.partitionBy("hashtaginformada","ano","mes","dia").mode("append").parquet('/usr/local/spark/resources/data/svr/parquet/tweets.parquet')
    print('bronze tweets.parquet atualiado')
else:
    print('Não ocorreu eliminação de distintos')

distinct_df.createOrReplaceTempView("tweets_view");
sql = "SELECT hashtaginformada, ano, mes, dia, \
count(*) qtd_tweets, sum(retweetCount) retweetCount, sum(likeCount) likeCount \
FROM tweets_view \
group by hashtaginformada, ano, mes, dia"

df_tweets_agg = spark.sql(sql);

#Gravar parquet - sempre overwrite
df_tweets_agg.write.mode('overwrite').parquet('/usr/local/spark/resources/data/svr/parquet/tweets_agg.parquet')
df_tweets_agg.write.partitionBy("hashtaginformada","ano").mode("append").parquet('/usr/local/spark/resources/data/svr/parquet/tweets_agg.parquet')
print('bronze tweets_agg.parquet gerado')    

df_tweets_agg.show()
#df_tweets_agg.printSchema()
#spark.stop()