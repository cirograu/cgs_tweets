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
df2 = df.withColumn('data', Func.to_timestamp(Func.substring('date', 1 , 10),"yyyy-MM-dd"))

#############################################
# Dimensional Período
#############################################

df2.createOrReplaceTempView("tweets_v");
df_periodo = spark.sql("SELECT DISTINCT data, ano, mes, dia FROM tweets_v ");
#df_periodo.show()

#Gravar parquet - sempre overwrite
df_periodo.write.mode('overwrite').parquet('/usr/local/spark/resources/data/gld/parquet/dim_periodo.parquet')
print('======================================')
print('Gold dim_periodo.parquet gerado')

#############################################
# Dimensional Usuário
#############################################

df.createOrReplaceTempView("tweets_v2");
sql = "SELECT  username, displayname, \
            max(followersCount) followersCount, max(friendsCount) friendsCount \
        FROM tweets_v2 \
        GROUP BY username, displayname  \
        ORDER BY username "
df_usuario = spark.sql(sql);
#df_usuario.show()

#Gravar parquet - sempre overwrite
df_usuario.write.mode('overwrite').parquet('/usr/local/spark/resources/data/gld/parquet/dim_usuario.parquet')
print('Gold dim_usuario.parquet gerado')

#############################################
# Dimensional Origem
#############################################

df.createOrReplaceTempView("tweets_v3");
sql = "SELECT DISTINCT UPPER(sourceLabel) sourceLabel \
        FROM tweets_v3 \
        GROUP BY sourceLabel  \
        ORDER BY sourceLabel "
df_origem = spark.sql(sql);

df_origem = df_origem.withColumn("origem",
                          Func.when(Func.substring('sourceLabel',1,7)=='TWITTER','TWITTER').
                           otherwise(df_origem.sourceLabel)
                          )
df_origem = df_origem.withColumn("origem",
                          Func.when(Func.substring('origem',1,9)=='TWEETDECK','TWEETDECK').
                           otherwise(df_origem.origem)
                          )
df_origem = df_origem.withColumn("origem",
                          Func.when(Func.substring('origem',1,9)=='FS POSTER','FS POSTER').
                           otherwise(df_origem.origem)
                          )


#Gravar parquet - sempre overwrite
df_origem.write.mode('overwrite').parquet('/usr/local/spark/resources/data/gld/parquet/dim_origem.parquet')
print('Gold dim_origem.parquet gerado')
print('======================================')