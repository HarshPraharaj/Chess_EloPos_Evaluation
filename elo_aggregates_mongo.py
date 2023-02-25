import json

from google.cloud import storage
from mongodb import *
from pyspark.sql import Row, SparkSession

from user_definition import *


def add_elo_csv_data_to_rdd(rdd):
    rdd_dict = rdd.asDict()
    id = rdd_dict['Player'] + rdd_dict['game_num']
    rdd_dict['_id'] = id
    # rdd_dict.pop('game_id', None)
    return rdd_dict


def insert_elo_aggregates_to_mongo():
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("google.cloud.auth.service.account.json.keyfile", service_account_key_file)
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

    games = (spark.read.format("csv").option("header", True).load(f"gs://{bucket_name}/elo_data/*.csv"))

    mongodb = MongoDBCollection(mongo_username,
                                mongo_password,
                                mongo_ip_address,
                                database_name.replace(" ", ""),
                                "elo_eval")

    aggs = games.rdd.map(lambda x: add_elo_csv_data_to_rdd(x)).collect()
    try:
        mongodb.insert_many(aggs)
    except:
        pass


if __name__ == "__main__":
    insert_elo_aggregates_to_mongo()
