package com.axa.dil

import com.axa.dil.models.Models.{Claim, Person}
import frameless.TypedDataset
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  */
trait WithSparkParquet {

  implicit  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark data best practices")
    .getOrCreate()

  import spark.implicits._

  val personsDF: DataFrame = spark.read.parquet("src/test/resources/data/persons")
  val personsDS: Dataset[Person] = personsDF.as[Person]
  val personsTD: TypedDataset[Person] = TypedDataset.create(personsDS)

  val claimsDF: DataFrame = spark.read.parquet("src/test/resources/data/claims")
  val claimsDS: Dataset[Claim] = claimsDF.as[Claim]
  val claimsTD: TypedDataset[Claim] = TypedDataset.create(claimsDS)

}
