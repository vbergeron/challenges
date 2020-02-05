package com.didomi.challenge.data

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {

  def getInput(spark: SparkSession, path: String) = {
    spark.read
      .json("./input")
      .dropDuplicates("id")
      .withColumn("date", substring(col("datehour"), 0, 10).cast(DateType))
      .withColumn("hour", substring(col("datehour"), 12, 14).cast(IntegerType))
  }

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
      .appName("didomi-data-challenge")
      .master("local[4]") // run in localmode
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR") // run in localmode

    getInput(spark, "input-exemple.zip")
      .groupBy($"date", $"hour", $"domain", $"user.country")
      .agg(
        countDistinct($"user.id") as "users",
        sum(($"type" === "pageview").cast("int")).alias("pageviews"),
        sum(($"type" === "pageview" and $"user.consent").cast("int")) as "pageviews_with_consent",
        sum(($"type" === "consent.asked").cast("int")) as "consents_asked",
        sum(($"type" === "consent.asked" and $"user.consent").cast("int")) as "consents_asked_with_consent",
        sum(($"type" === "consent.given").cast("int")) as "consents_given",
        sum(($"type" === "consent.given" and $"user.consent").cast("int")) as "consents_given_with_consent"
      )
      .show()

    spark.close()
  }

}
