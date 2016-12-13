package com.axa.dil

import com.axa.dil.models.Models._
import frameless.TypedDataset
import org.apache.spark.sql.Dataset
import org.scalatest.FlatSpec

/**
  *
  */
class ComparisonSpec extends FlatSpec with WithSparkParquet {

  "select column with dataset" should "pass" in {
    val names: Dataset[PersonName] = SelectColumn.withDataset(personsDS)
    names.show()
  }

  "select column with frameless" should "pass" in {
    val names: TypedDataset[PersonName] = SelectColumn.withFrameless(personsTD)
    names.show().run()
  }


  "new column with dataset" should "pass" in {
    val isAdult: Dataset[PersonIsAdult] = NewColumn.withDataset(personsDS)
    isAdult.show()
  }

  "new column with frameless" should "pass" in {
    val isAdult: TypedDataset[PersonIsAdult] = NewColumn.withFrameless(personsTD)
    isAdult.show().run()
  }


  "filter with dataset" should "pass" in {
    val ageDS: Dataset[Long] = Filter.withDataset(personsDS)
    ageDS.explain()
    ageDS.show()
  }

  "filter with frameless" should "pass" in {
    val ageTD = Filter.withFrameless(personsTD)
    ageTD.explain()
    ageTD.show().run()
  }

  "filter with dataframe" should "pass" in {
    val ageTD = Filter.withDataframe(personsDF)
    ageTD.explain()
    ageTD.show()
  }


  "join with dataset" should "pass" in {
    val personClaims: Dataset[PersonClaimFlat] = Join.withDataset(personsDS, claimsDS)
    personClaims.explain()
    personClaims.show()
  }

  "join with frameless" should "pass" in {
    val personClaims: TypedDataset[PersonClaim] = Join.withFrameless(personsTD, claimsTD)
    personClaims.explain()
    personClaims.show().run()
  }


  "aggregate with dataset" should "pass" in {
    val averageAge: Dataset[PersonAvgAge] = Aggregate.withDataset(personsDS)
    averageAge.explain()
    averageAge.show()
  }

  "aggregate with frameless" should "pass" in {
    val averageAge: TypedDataset[PersonAvgAge] = Aggregate.withFrameless(personsTD)
    averageAge.explain()
    averageAge.show().run()
  }


  "aggregate then join with dataset" should "pass" in {
    val totalPrice: Dataset[ClaimWithTotalPriceFlat] = AggregateThenJoin.withDataset(claimsDS)

    totalPrice.show()
  }

  "aggregate then join with frameless" should "pass" in {
    val totalPrice: TypedDataset[ClaimWithTotalPrice] = AggregateThenJoin.withFrameless(claimsTD)

    totalPrice.show().run()
  }

  "data encode with dataset" should "pass" in {
    DateEncode.withDataset()
  }

  "data encode with frameless" should "pass" in {
    DateEncode.withFrameless()
  }

}
