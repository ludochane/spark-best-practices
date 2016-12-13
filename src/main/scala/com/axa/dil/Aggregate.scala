package com.axa.dil

import com.axa.dil.models.Models.{Person, PersonAvgAge}
import frameless.TypedDataset
import frameless.functions.aggregate.{avg => framelessAvg}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

/**
  *
  */
object Aggregate extends Sparkable {

  import spark.implicits._

  def withDataset(persons: Dataset[Person]): Dataset[PersonAvgAge] = {
    persons
      .groupBy("name")
      .agg(avg("age").as("age"))
      .as[PersonAvgAge]
  }

  def withFrameless(persons: TypedDataset[Person]): TypedDataset[PersonAvgAge] = {
    persons
      .groupBy(persons('name))
      .agg(framelessAvg(persons('age)))
      .as[PersonAvgAge]
  }

}
