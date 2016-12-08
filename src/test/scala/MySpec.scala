import com.axa.dil.models.Models._
import com.axa.dil._
import frameless.TypedDataset
import org.apache.spark.sql.Dataset
import org.scalatest.FlatSpec

/**
  *
  */
class MySpec extends FlatSpec with WithSparkData {

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
    ageDS.show()
  }

  "filter with frameless" should "pass" in {
    val ageTD = Filter.withFrameless(personsTD)

    ageTD.show().run()
  }


  "join with dataset" should "pass" in {
    val personClaims: Dataset[PersonClaimFlat] = Join.withDataset(personsDS, claimsDS)

    personClaims.show()
  }

  "join with frameless" should "pass" in {
    val personClaims: TypedDataset[PersonClaim] = Join.withFrameless(personsTD, claimsTD)

    personClaims.show().run()
  }


  "aggregate with dataset" should "pass" in {
    val averageAge: Dataset[PersonAvgAge] = Aggregate.withDataset(personsDS)

    averageAge.show()
  }

  "aggregate with frameless" should "pass" in {
    val averageAge: TypedDataset[PersonAvgAge] = Aggregate.withFrameless(personsTD)

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

}
