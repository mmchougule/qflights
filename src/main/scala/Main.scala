package org.quantexa.flights
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import org.joda.time.LocalDate

import java.sql.Date

object Main {
  private def logger(): Logger = {
    val log: Logger = org.apache.log4j.LogManager.getLogger(this.getClass)
    log.setLevel(org.apache.log4j.Level.ERROR)
    log
  }

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .config("spark.master", "local")
      .appName("Flights").getOrCreate()

    import spark.implicits._
    // hide spark info logs
    spark.sparkContext.setLogLevel("ERROR")
    val log = logger()

    // set value for data path to be implicit so we don't have to pass it to functions
    implicit val dataPath: String = sys.env("DATA_PATH")

    // use this dataPath as implicit variable in functions
    val flightDataDf = spark.read.option("header", value = true).csv(s"${dataPath}/flightData.csv")
    flightDataDf.show()
    val passengersDf = spark.read.option("header", value = true).csv(s"${dataPath}/passengers.csv")
    passengersDf.show()

    log.debug("Solving question 1")

    // solve question 1
    val solvedDfQ1 = solveQuestion1(flightDataDf)
    save(solvedDfQ1, "question1")

    // avoid passing spark session to solveQuestion2

    // solve question 2
    val solvedDfQ2 = solveQuestion2(flightDataDf, passengersDf)
    save(solvedDfQ2, "question2")

    // solve question 3
    val solvedDfQ3 = solveQuestion3(spark, flightDataDf.filter($"passengerId" === "2260"))
    save(solvedDfQ3, "question3")

    solvedDfQ3.show()
    // solve question 4
    val from = "2017-04-01"
    val to = "2017-05-01"

    val fromSqlDate = getDate(from)
    val toSqlDate = getDate(to)

    val flownDf = flownTogether(spark, flightDataDf, 3, fromSqlDate,toSqlDate)
    save(flownDf, "question4")
    flownDf.show()
    spark.stop()
  }


  private def save(df: DataFrame, question: String, n: Int = 1) (implicit dataPath: String): Unit = {
    /*
    Save the results of a question to a csv file
     */
    logger().info(s"Saving results for question $question")
    // use implicit dataPath variable specified in the main file, replace if file exists already
    df.coalesce(n).write.mode("overwrite").csv(s"${dataPath}/$question.csv")
  }

  def solveQuestion3(spark: SparkSession, flightDataDf: DataFrame, cFilter: String = "uk"): DataFrame = {
    /*
    Question 3
    Find the greatest number of countries a passenger has been in without being in the UK.
    e.g. UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.

    input: flightDataDf

    output: dataframe with columns
    PassengerID, Longest Run
    */

    import spark.implicits._
    import org.apache.spark.sql.expressions.Window

    val flightsDf = flightDataDf.orderBy("passengerId", "date")
    val window = Window.partitionBy("passengerId").orderBy("date")
    val ukArrivals = flightsDf.withColumn("countUk", sum(
      when(col("from") === "uk", 1).otherwise(0)
    ).over(window))

    // get the number of non UK countries that passengers visited to
    ukArrivals
      .groupBy("passengerId", "countUk")
      .agg((
        sum(
          when(
            $"from" =!= cFilter && $"to" =!= cFilter, 1
          ).when(
            $"from" === $"to", -1
          ).otherwise(0)
        ) + 1
        ).as(s"noVisit$cFilter"))
      .groupBy("passengerId")
      .agg(max("noVisitUk").alias("Longest Run"))
  }

  def flownTogether(spark: SparkSession,
                            flightDataDf: DataFrame, atLeastNTimes: Int, from: Date, to: Date): DataFrame = {
    /*
    Question 4
    Find the passengers who have been on more than 3 flights together.

    input: flightDataDf

    output: dataframe with columns
    PassengerId1, PassengerId2, Number of flights together
    */

    import spark.implicits._

    val flown = flightDataDf.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    flown.as("firstDf").withColumnRenamed("passengerId", "passengerId1")
      .join(flown.as("secondDf").withColumnRenamed("passengerId", "passengerId2"),
      $"passengerId1" =!= $"passengerId2" &&
      $"firstDf.flightId" === $"secondDf.flightId" &&
      $"firstDf.date" === $"secondDf.date")
      .filter($"firstDf.date" >= from && $"firstDf.date" <= to)
      .groupBy("passengerId1", "passengerId2")
      .agg(
        count("firstDf.flightId").as("Number of flights together"),
        min($"firstDf.date").as("From"),
        max($"firstDf.date").as("To")
      ).orderBy($"From".asc)
      .filter($"Number of flights together" >= atLeastNTimes)

  }

  def solveQuestion2(flightDataDf: DataFrame, passengersDf: DataFrame): DataFrame = {
    /*
    Question 2
    Find the names of the 100 most frequent flyers.

    input: flightDataDf, passengersDf

    output: dataframe with columns
    PassengerId, Number of flights, First Name, Last Name
     */
    flightDataDf
      .join(passengersDf, "passengerId")
      .groupBy("PassengerId")
      .agg(count("flightId").as("Number of flights"),
        first("FirstName").as("First Name"),
        first("LastName").as("Last Name")
      )
      .orderBy(desc("Number of flights"))
      .limit(10)
  }

  def solveQuestion1(flightDataDf: DataFrame): DataFrame = {
    /*
    Question 1
    Find the total number of flights for each month.

    input: flightDataDf

    output: dataframe with columns
    Month, Total number of flights
     */
    flightDataDf.select("flightId", "date")
      .distinct()
      .withColumn("month", month(col("date")))
      .withColumn("year", year(col("date")))
      .groupBy("year", "month")
      .agg(count("flightId").as("Total number of flights"))
      .orderBy("month")

  }

  def getDate(date: String): Date = {
    val dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd")
    val fromLocalDate = LocalDate.parse(date, dateFormat)
    new Date(fromLocalDate.toDateTimeAtStartOfDay.getMillis)
  }
}