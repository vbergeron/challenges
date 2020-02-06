package com.didomi.challenge.data

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {

  def withSession[T](run: SparkSession => T): T = {
    val spark = SparkSession.builder
      .appName("didomi-data-challenge")
      .master("local[4]") // run in localmode
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") // run in localmode
    val ret = run(spark)
    spark.close()
    ret
  }

  def getInput(spark: SparkSession, path: String): Dataset[Event] = {
    import spark.implicits._
    spark.read
      .json(path)
      .as[Event]
  }

  def preprocess(in: Dataset[Event]): Dataset[Event] =
    in.dropDuplicates("id")

  def report(in: Dataset[Event]): Dataset[Row] =
    in.groupBy(
        col("timestamp").cast("date").as("date"),
        hour(col("timestamp")).as("hour"),
        col("domain"),
        col("user.country")
      )
      .agg(
        countDistinct(col("user.id")).as("users"),
        sum(
          (col("type") === "pageview").cast("int")
        ).as("pageviews"),
        sum(
          (col("type") === "pageview" and col("user.consent")).cast("int")
        ) as "pageviews_with_consent",
        sum(
          (col("type") === "consent.asked").cast("int")
        ) as "consents_asked",
        sum(
          (col("type") === "consent.asked" and col("user.consent")).cast("int")
        ) as "consents_asked_with_consent",
        sum(
          (col("type") === "consent.given").cast("int")
        ) as "consents_given",
        sum(
          (col("type") === "consent.given" and col("user.consent")).cast("int")
        ) as "consents_given_with_consent"
      )

  def main(args: Array[String]): Unit =
    withSession(spark => {
      import spark.implicits._

      getInput(spark, "./input")
        .transform(preprocess)
        .transform(report)
        .show()
    })
  //.show()

  //spark.close()
}
