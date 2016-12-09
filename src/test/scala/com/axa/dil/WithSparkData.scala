package com.axa.dil

import com.axa.dil.models.Models.{Claim, Person}
import frameless.TypedDataset
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  */
trait WithSparkData {

  implicit  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Spark data best practices")
    .getOrCreate()

  import spark.implicits._

  val personsDF: DataFrame = Seq(
    Person("p1", "Julia", 14),
    Person("p2", "Marc", 29),
    Person("p3", "Andrew", 18),
    Person("p4", "Julia", 80)
  ).toDF

  val personsDS: Dataset[Person] = personsDF.as[Person]

  val personsTD: TypedDataset[Person] = TypedDataset.create(personsDS)

  val claimsDS: Dataset[Claim] = Seq(
    Claim("c1", "p1", 250.25),
    Claim("c2", "p1", 10.15),
    Claim("c3", "p2", 1200.30)
  ).toDS()

  val claimsTD: TypedDataset[Claim] = TypedDataset.create(claimsDS)

}
