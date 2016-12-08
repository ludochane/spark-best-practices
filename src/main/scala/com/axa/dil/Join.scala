package com.axa.dil

import com.axa.dil.models.Models.{Claim, Person, PersonClaim, PersonClaimFlat}
import frameless.TypedDataset
import org.apache.spark.sql.Dataset

/**
  *
  */
object Join extends Example {

  import spark.implicits._

  def withDataset(persons: Dataset[Person], claims: Dataset[Claim]): Dataset[PersonClaimFlat] = {
    val claimsRenamed = claims.withColumnRenamed("id", "claimId")

    val personClaims: Dataset[PersonClaimFlat] = persons
      .join(claimsRenamed, persons("id") === claimsRenamed("personId"))
      .as[PersonClaimFlat]

    personClaims
  }


  def withFrameless(persons: TypedDataset[Person], claims: TypedDataset[Claim]): TypedDataset[PersonClaim] = {
    val personClaimsJoinDS: TypedDataset[PersonClaim] = persons
      .join(claims, persons('id), claims('personId))
      .as[PersonClaim]

    personClaimsJoinDS
  }

}
