package org.quantexa.flights
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.quantexa.flights.Main.{flownTogether, getDate, solveQuestion1, solveQuestion2, solveQuestion3}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
class FlightsTestSpec extends  AnyFunSuite {

  val logger: Logger = Logger.getLogger(this.getClass)
  logger.setLevel(org.apache.log4j.Level.ERROR)

  test("test solveQuestion1") {
    // create a dataframe
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val flightDf = Seq((1, "2023-01-01"), (2, "2023-01-15"), (3, "2022-12-01")).toDF("flightId", "date")

    val expectedDf = Seq((2023, 1, 2), (2022, 12, 1)).toDF("year", "month", "Total number of flights")
    val result = solveQuestion1(flightDf)
    flightDf.show()
    expectedDf.show()
    result.show()
    assert(result.collect().sameElements(expectedDf.collect()))
  }
  test("test solveQuestion2") {
    // create a dataframe
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val flightDataDf = Seq((1, 1), (2, 1), (3, 2)).toDF("flightId", "passengerId")
    val passengersDf = Seq((1, "Jaylen", "Brown"), (2, "Jayson", "Tatum")).toDF("passengerId", "FirstName", "LastName")

    val expectedResult = Seq((1, 2, "Jaylen", "Brown"), (2, 1, "Jayson", "Tatum")).toDF("PassengerId", "Number of flights", "First Name", "Last Name")
    val result = solveQuestion2(flightDataDf, passengersDf)

    assert(result.collect().sameElements(expectedResult.collect()))
  }

  test("test solveQuestion3") {
    logger.setLevel(org.apache.log4j.Level.ERROR)
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val flightDataDf = Seq((1, "uk", "fr", "2022-12-01"), (1, "fr", "us", "2022-12-02"),
      (1, "us", "cn", "2022-12-10"), (1, "cn", "uk", "2023-01-01"),
      (1, "uk", "de", "2023-02-01"), (1, "de", "uk", "2022-01-10"))
      .toDF("PassengerID", "from", "to", "date")

    val expectedResult = Seq((1, 3)).toDF("PassengerID", "Longest Run")
    val result = solveQuestion3(spark, flightDataDf)

    flightDataDf.show()
    expectedResult.show()
    result.show()
    assert(result.collect().sameElements(expectedResult.collect()))
  }

  test("flownTogether returns an empty dataframe when no passengers match the criteria") {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._

    val flightDataDf = Seq(
      (1, "flight1", "2023-01-01", "country1", "country2"),
      (2, "flight2", "2023-02-01", "country2", "country3"),
      (3, "flight3", "2022-12-01", "country3", "country1")
    ).toDF("passengerId", "flightId", "date", "from", "to")


    val from = "2022-12-01"
    val to = "2023-02-01"
    val fromSqlDate = getDate(from)
    val toSqlDate = getDate(to)
    val result = flownTogether(spark, flightDataDf, atLeastNTimes = 3, from = fromSqlDate, to = toSqlDate)

    result.count() should be (0)
  }
}
