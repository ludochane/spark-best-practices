package com.axa.dil

import com.axa.dil.models.Models.{Person, PersonIsAdult}
import frameless.TypedDataset
import org.apache.spark.sql.Dataset

/**
  *
  */
object NewColumn extends Example {

  import spark.implicits._

  def withDataset(persons: Dataset[Person]): Dataset[PersonIsAdult] = {
    persons.withColumn("isAdult", persons("age") >= 18).as[PersonIsAdult]
  }

  def withDatasetMap(persons: Dataset[Person]): Dataset[PersonIsAdult] = {
    persons.map {
      case Person(id, name, age) => PersonIsAdult(id, name, age, age >= 18)
    }
  }

  def withFrameless(persons: TypedDataset[Person]): TypedDataset[PersonIsAdult] = {
    // no withColumn function
    // use UDF
    val isAdult = (age: Long) => age >= 18
    val isAdultUDF = persons.makeUDF(isAdult)

    val personIsAdultTD: TypedDataset[PersonIsAdult] = persons.select(
      persons('id),
      persons('name),
      persons('age),
      isAdultUDF(persons('age))
    ).as[PersonIsAdult]

    personIsAdultTD
  }
}
