package com.axa.dil

import com.axa.dil.models.Models.{Claim, ClaimWithTotalPrice, ClaimWithTotalPriceFlat, TotalPriceByPersonId}
import frameless.TypedDataset
import frameless.functions.aggregate.{sum => framelessSum}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

/**
  *
  */
object AggregateThenJoin extends Example {

  import spark.implicits._

  def withDataset(claims: Dataset[Claim]): Dataset[ClaimWithTotalPriceFlat] = {
    val totalPrices = claims
      .groupBy(claims("personId"))
      .agg(sum(claims("price")).as("total"))
      .as[TotalPriceByPersonId]

    val claimWithTotalPrice = claims
      .join(totalPrices, "personId")
      .as[ClaimWithTotalPriceFlat]

    claimWithTotalPrice
  }

  def withFrameless(claims: TypedDataset[Claim]): TypedDataset[ClaimWithTotalPrice] = {
    val totalPrices = claims
      .groupBy(claims('personId))
      .agg(framelessSum(claims('price)))
      .as[TotalPriceByPersonId]

    val claimWithTotalPrice = claims
      .join(totalPrices, claims('personId), totalPrices('personId))
      .as[ClaimWithTotalPrice]

    claimWithTotalPrice
  }

}
