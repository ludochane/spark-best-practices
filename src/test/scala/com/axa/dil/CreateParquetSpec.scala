package com.axa.dil

import org.scalatest.FlatSpec

/**
  *
  */
class CreateParquetSpec extends FlatSpec with WithSparkData {

  it should "create Parquet" in {
    personsDS.repartition(1).write.parquet("src/test/resources/data/persons")
    claimsDS.repartition(1).write.parquet("src/test/resources/data/claims")
  }

}
