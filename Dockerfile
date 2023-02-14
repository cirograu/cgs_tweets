FROM apache/airflow:2.5.1-python3.8
#apt-get update && apt-get install -y git
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64


RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3


RUN pip install --no-cache-dir apache-airflow-providers-apache-spark

RUN pip install snscrape --quiet
########### rodar depois  ############################################
#RUN python -m pip install git+https://github.com/tobe93gf/snscrape.git --quiet
######################################################################
#RUN python -m  pip install --upgrade git+https://github.com/JustAnotherArchivist/snscrape@master
#RUN python -m pip install git+https://github.com/JustAnotherArchivist/snscrape.git


RUN pip install pyspark --quiet
#RUN pip3 install psycopg2 --quiet

#Airflow
RUN pip install apache-airflow --quiet
RUN pip install apache-airflow-providers-apache-spark --quiet

#ler notebooks no Airflow
RUN pip install papermill --quiet 
RUN pip install apache-airflow-providers-papermill --quiet

RUN pip install ipykernel


