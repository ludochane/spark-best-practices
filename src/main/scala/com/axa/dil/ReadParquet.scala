package com.axa.dil

import com.axa.dil.models.Models.Person
import frameless.TypedDataset
import org.apache.spark.sql.{Dataset, _}

/**
  *
  */
object ReadParquet extends Sparkable {

  import spark.implicits._

  def withDataset(path: String): Dataset[Person] = {
    val personDF: DataFrame = spark.read.parquet(path)

    val personsDS: Dataset[Person] = personDF.as[Person]

    personsDS
  }

  def withFrameless(path: String): TypedDataset[Person] = {
    val personDF: DataFrame = spark.read.parquet(path)

    val personsDS: Dataset[Person] = personDF.as[Person]

    val personsTD: TypedDataset[Person] = TypedDataset.create(personsDS)

    personsTD
  }


}
