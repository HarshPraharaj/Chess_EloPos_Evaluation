import os

import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pandas as pd
import logging

from user_definition import *
from helper import *


def create_initial_dir(**kwargs):
    if not os.path.isdir(kwargs['data_dir']):
        os.mkdir(kwargs['data_dir'])


# def retrieve_player_games(username):
#     response = get_user_games(username)
#     user_games = user_games_to_pgn(response)
#     save_as_csv(user_games)
#     return True

def retrieve_player_games(**kwargs):
    for username in kwargs['user_name']:
        response = get_user_games(username)
        # logging.info(f"response received {response}")
        user_games = user_games_to_pgn(response)
        games_df = make_dataframe(user_games)
        push_df_gcp(games_df, username, kwargs['bucket_name'], kwargs['service_account_key_file'])


def retrieve_elo_ratings(**kwargs):
    push_elo_to_gcp(kwargs['elo_dir'], kwargs['bucket_name'], kwargs['service_account_key_file'])


# print(username)
# userg = retrieve_player_games(username)

with DAG(dag_id="pipeline",
         start_date=datetime(2023, 2, 24),
         schedule_interval='@daily') as dag:
    # https://github.com/apache/airflow/discussions/24463
    os.environ["no_proxy"] = "*"  # set this for airflow errors.

    # create_dir = PythonOperator(task_id = "create_dir",
    #             python_callable = create_initial_dir,
    #             op_kwargs = {'data_dir':data_dir})

    # create_dirs_op = BashOperator(task_id="create_dirs_op",
    #                               bash_command=f"mkdir -p {data_dir}/{username}")

    retrieve_player_games_ascsv = PythonOperator(task_id="retrieve_player_games_ascsv",
                                                 python_callable=retrieve_player_games,
                                                 op_kwargs={'user_name': usernames, 'bucket_name': bucket_name,
                                                            'service_account_key_file': service_account_key_file})

    retrieve_elo_ratings_csv = PythonOperator(task_id="retrieve_elo_ratings_csv",
                                              python_callable=retrieve_elo_ratings,
                                              op_kwargs={'elo_dir': elo_ratings_dir, 'bucket_name': bucket_name,
                                                         'service_account_key_file': service_account_key_file})

    create_insert_aggregate = SparkSubmitOperator(
        task_id="aggregate_creation",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={"spark.driver.userClassPathFirst": True,
              "spark.executor.userClassPathFirst": True,
              #  "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
              #  "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
              #  "spark.hadoop.fs.gs.auth.service.account.enable":True,
              #  "google.cloud.auth.service.account.json.keyfile":service_account_key_file,
              },
        verbose=True,
        application='aggregates_to_mongo.py'
    )

    create_insert_aggregate_elo = SparkSubmitOperator(
        task_id="elo_aggregate_creation",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={"spark.driver.userClassPathFirst": True,
              "spark.executor.userClassPathFirst": True,
              #  "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
              #  "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
              #  "spark.hadoop.fs.gs.auth.service.account.enable":True,
              #  "google.cloud.auth.service.account.json.keyfile":service_account_key_file,
              },
        verbose=True,
        application='elo_aggregates_mongo.py'
    )

    retrieve_player_games_ascsv >> retrieve_elo_ratings_csv >> create_insert_aggregate >> create_insert_aggregate_elo
