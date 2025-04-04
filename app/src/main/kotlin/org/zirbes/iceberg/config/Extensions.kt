package org.zirbes.iceberg.config

import org.apache.spark.sql.SparkSession.Builder

fun Builder.setHadoop(k: String, v: String) =
    this.config("spark.hadoop.$k", v)
