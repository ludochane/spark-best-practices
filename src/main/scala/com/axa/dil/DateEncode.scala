package com.axa.dil

import frameless.TypedDataset
import org.apache.spark.sql.Dataset
import org.joda.time.DateTime
import com.axa.dil.encoders.Injections._

/**
  *
  */
object DateEncode extends Sparkable {

  import spark.implicits._

  case class PersonBirth(name: String, birthdate: DateTime)

  def withDataset(): Dataset[PersonBirth] = {
    spark.createDataset(Seq(
      PersonBirth("Sophie", new DateTime())
    ))
  }

  def withFrameless(): TypedDataset[PersonBirth] = {
    TypedDataset.create(Seq(
      PersonBirth("Sophie", new DateTime())
    ))
  }

}
