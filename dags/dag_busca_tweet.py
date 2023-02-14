import airflow

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

data_inicial  = '{{ macros.ds_add(ds, -30) }}'
data_final = '{{ ds }}'

argumentos = {
    'owner': 'airflow',
    'start_date' : airflow.utils.dates.days_ago(1),
}

def f_hashtag():
    v_hashtag = Variable.get("v_hashtag")
    print('HashTag recebida: ' + v_hashtag)
    return v_hashtag

def f_qtd_linhas():
    v_qtd_linhas = Variable.get("v_qtd_linhas")
    print('HashTag recebida: ' + v_qtd_linhas)
    return v_qtd_linhas

with DAG(
    dag_id = 'busca_tweet',
    default_args = argumentos,
    schedule_interval="@once"
) as dag:
    
    task = PythonOperator(
        task_id="display_variable",
        python_callable = f_hashtag
    ) 
         
    start_task = EmptyOperator(
        task_id='start'
    )

    comando = f'python /opt/airflow/dags/python/brz_01_busca_tweet_grava_json.py {Variable.get("v_hashtag")} {data_inicial} {data_final} {Variable.get("v_qtd_linhas")}'  
    elt_busca_tweet = BashOperator(
        task_id='elt_busca_tweet',
        bash_command=comando
    )

    comando = f'python /opt/airflow/dags/python/brz_02_ler_json_gravar_parquet.py {Variable.get("v_hashtag")} {data_inicial} {data_final}' 
    elt_grava_tweet = BashOperator(
        task_id='elt_grava_tweet',
        bash_command=comando
    )

    elt_gera_silver = BashOperator(
        task_id='elt_gera_silver',
        bash_command='python /opt/airflow/dags/python/svr_01_gera_silver.py'
    )

    elt_gera_gold_dimensionais = BashOperator(
        task_id='elt_gera_gold_dimensionais',
        bash_command='python /opt/airflow/dags/python/gld_01_dimensionais.py'
    )

    elt_gera_gold_fato = BashOperator(
        task_id='elt_gera_gold_fato',
        bash_command='python /opt/airflow/dags/python/gld_02_fato_tweets.py'
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> elt_busca_tweet
elt_busca_tweet >> elt_grava_tweet
elt_grava_tweet >> elt_gera_silver
elt_gera_silver >> elt_gera_gold_dimensionais
elt_gera_gold_dimensionais >> elt_gera_gold_fato
elt_gera_gold_fato >> end_task