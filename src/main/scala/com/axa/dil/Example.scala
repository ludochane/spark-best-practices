package com.axa.dil

import org.apache.spark.sql.SparkSession

/**
  *
  */
trait Example {

  implicit  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark data best practices")
    .getOrCreate()

  implicit val sqlContext = spark.sqlContext

}
