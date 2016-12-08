package com.axa.dil

import com.axa.dil.models.Models.Person
import frameless.{Injection, TypedDataset}
import org.apache.spark.sql._

/**
  *
  */
object Filter extends Example {

  import spark.implicits._

  def withDataset(persons: Dataset[Person]): Dataset[Long] = {
    // no Spark optimization
    // from Parquet, will read every columns, not only age
    persons.filter(_.age == 18).map(_.age)
  }

  def withFrameless(persons: TypedDataset[Person]) = {
    // with Spark optimization
    persons
      .filter(persons('age) === 18)
      .select(persons('age))
  }

  def withDataframe(dataframe: DataFrame): DataFrame = {
    // with Spark optimization
    // from parquet, will read only column age
    import spark.sqlContext.implicits._

    dataframe.filter($"age" > 18).select($"age")
  }

}
