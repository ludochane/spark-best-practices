package com.axa.dil

import com.axa.dil.models.Models.{Person, PersonName}
import frameless.TypedDataset
import org.apache.spark.sql.Dataset

/**
  *
  */
object SelectColumn extends Example {

  import spark.implicits._

  def withDataset(persons: Dataset[Person]): Dataset[PersonName] = {
    val personNameDS: Dataset[PersonName] = persons.select("id", "name").as[PersonName]

    personNameDS
  }

  def withFrameless(persons: TypedDataset[Person]): TypedDataset[PersonName] = {
    val personNameDS: TypedDataset[PersonName] = persons
      .select(persons.col('id), persons.col('name))
      .as[PersonName]

    personNameDS
  }
}
