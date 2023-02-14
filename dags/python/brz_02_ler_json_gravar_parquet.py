import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as Func
from pyspark.sql.functions import lit
from datetime import datetime
hoje = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Cria contexto Spark
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder \
    .master('local') \
    .appName('TwitterApp') \
    .config('spark.executor.memory', '4gb') \
    .getOrCreate()

contexto = spark.sparkContext
sqlContext = SQLContext(contexto)

# Estas variáveis serão passadas através de Parâmetros  (sys.argv[x])
########################################
print("informe a Twitter HashTag: #")
#hashtag = 'gremio'
hashtag = sys.argv[1]
desde = sys.argv[2]
ate = sys.argv[3]
########################################

# Le arquivo json
ambiente = "/usr/local/spark/resources"
diretorio = '/data/brz/json/'
file_json = f'{ambiente}{diretorio}{hashtag}-tweets.json'
df = spark.read.json(file_json)

print(f'json lido: {df.count()} registros')

if df.count() > 0 :
    df2 = df.withColumn('ano', Func.substring('date', 1 , 4).cast("int"))\
      .withColumn('mes', Func.substring('date', 6 , 2).cast("int"))\
      .withColumn('dia', Func.substring('date', 9 , 2).cast("int"))\
      .withColumn("hashtaginformada", lit(hashtag).cast("string")) \
      .withColumn("retweetCount",Func.col("retweetCount").cast("int")) \
      .withColumn("likeCount",Func.col("likeCount").cast("int")) 

    df2.createOrReplaceTempView("tweets_view");
    #df2.printSchema()
    sql = "SELECT ano, mes, dia, hashtaginformada, \
        id, retweetCount, likeCount, lang, sourceLabel, \
        rawContent texto, date, hashtags,\
        user.location,  \
        user.displayname , user.username, user.followersCount, user.friendsCount  \
        FROM tweets_view "

    df_sql = spark.sql(sql);

    #Gravar parquet
    diretorio = '/data/brz/parquet/'
    caminho_parquet = f'{ambiente}{diretorio}tweets.parquet'

    try:
        df_brz = sqlContext.read.parquet('/usr/local/spark/resources/data/brz/parquet/tweets.parquet')
        df_sql.write.mode('append').parquet(caminho_parquet)
        print('Silver tweets.parquet atualizado')
    except:
        #Gravar parquet - sempre overwrite
        df_sql.write.mode('overwrite').parquet(caminho_parquet)
        print('Silver tweets.parquet gerado')

    v_qtd_registros = df.count()    
    print(f'parquet: {df.count()} registros')
    #df_show = spark.sql("SELECT hashtaginformada , count(*) as qtd  FROM tweets_view2 group by 1");
    #df_show.show()
    #df_brz.count()

else:
    v_qtd_registros = 0    
    print('Json vazio')
    

# Cria Log
df2.createOrReplaceTempView("tweets_view");

df_log = spark.sql("SELECT DISTINCT hashtaginformada as hashtag FROM tweets_view");
df_log.show()

if v_qtd_registros == 0:
    df_log = df_log.withColumn('hashtag', lit(hashtag))
    df_log.show() 

df_log2 = df_log.withColumn('desde', lit(desde))\
  .withColumn('ate', lit(ate))\
  .withColumn('data_execucao', lit(hoje))\
  .withColumn('qtd_registros', lit(v_qtd_registros ))

diretorio = '/data/brz/parquet/'
caminho_parquet = f'{ambiente}{diretorio}log_tweets.parquet'

try:
    df_brz = sqlContext.read.parquet('/usr/local/spark/resources/data/brz/parquet/log_tweets.parquet')
    df_log2.write.mode('append').parquet(caminho_parquet)
    print('Bronze log_tweets.parquet atualizado')
except:
    df_log2.write.mode('overwrite').parquet(caminho_parquet)
    print('Bronze log_tweets.parquet gerado')