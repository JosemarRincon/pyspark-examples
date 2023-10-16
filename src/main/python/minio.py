# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A simple example demonstrating basic Spark SQL features.
Run with:
  ./bin/spark-submit examples/src/main/python/sql/basic.py
"""
# $example on:init_session$
from pyspark.sql import SparkSession
# $example off:init_session$

# $example on:schema_inferring$
from pyspark.sql import Row
# $example off:schema_inferring$

# $example on:programmatic_schema$
# Import data types
from pyspark.sql.types import StringType, StructType, StructField
# $example off:programmatic_schema$
import os
   # Get the Spark logger


if __name__ == "__main__":
    # $example on:init_session$
    # Recupere uma variável de ambiente específica
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT")



    spark = SparkSession \
        .builder \
        .appName("Pyspark com Minio") \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    # $example off:init_session$
    # Configure the AWS access and secret keys
    logger = spark._jvm.org.apache.log4j
    log = logger.LogManager.getLogger(__name__)

    # Set the log level (optional, if needed)
    # You can set the log level to one of: "OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL"
    log.setLevel(logger.Level.INFO)

    log.info(access_key)
    log.info(secret_key)
    log.info(minio_endpoint)

    # Set the MinIO endpoint
    # spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio-server:9000")

    # Use the S3A URL to read data
    df_lista_cns = spark.read.parquet("s3a://cns/lista_cns")

 

    # Log some messages
    log.info(df_lista_cns.show())

   


    spark.stop()