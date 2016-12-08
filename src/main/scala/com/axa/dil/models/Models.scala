package com.axa.dil.models

/**
  *
  */
object Models {
  case class Person(id: String, name: String, age: Long)
  case class PersonName(id: String, name: String)
  case class PersonAge(person: Person, age: Long)
  case class PersonAvgAge(name: String, age: Double)
  case class PersonIsAdult(id: String, name: String, age: Long, isAdult: Boolean)
  case class Claim(id: String, personId: String, price: Double)
  case class PersonClaimFlat(id: String, name: String, age: Long, claimId: String, personId: String, price: Double)
  case class PersonClaim(person: Person, claim: Claim)

  case class TotalPriceByPersonId(personId: String, total: Double)
  case class ClaimWithTotalPrice(claim: Claim, totalPriceByPersonId: TotalPriceByPersonId)
  case class ClaimWithTotalPriceFlat(id: String, personId: String, price: Double, total: Double)
}
