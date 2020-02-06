package com.didomi.challenge.data
import org.scalatest._
import org.apache.spark.sql._
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset
class TestReports extends FlatSpec {
  def base(): Event = Event.mock("pageview", User("1", false, "FR"))

  def assertFirstLineCol[T](df: Dataset[Row], col: String, value: T): Unit =
    assert(df.select(col).collect()(0).getAs[T](0) == value)

  it should "deduplicate entries based on event ID" in {
    Main.withSession((spark) => {
      import spark.implicits._

      val ev = base()
      val data: Dataset[Event] = spark.createDataset(
        Seq(ev, ev.copy())
      )
      val res = Main.preprocess(data)
      assert(data.count() == 2)
      assert(res.count() == 1)
    })
  }
  it should "group on date hour domain and user country" in {
    val data =
      Seq(
        base(),
        base().copy(
          timestamp = Timestamp.from(
            LocalDateTime
              .now()
              .minusDays(1)
              .toInstant(ZoneOffset.UTC)
          )
        ),
        base().copy(
          timestamp = Timestamp.from(
            LocalDateTime
              .now()
              .minusDays(1)
              .minusHours(1)
              .toInstant(ZoneOffset.UTC)
          )
        ),
        base().copy(domain = "www.other.website.com"),
        base().copy(user = User("2", true, "US"))
      )

    Main.withSession((spark) => {
      import spark.implicits._
      val res = spark
        .createDataset(data)
        .transform(Main.preprocess)
        .transform(Main.report)

      assert(res.count() == data.size)

    })
  }
  it should "compute number of unique users" in {
    val data = Seq(
      base(),
      base().copy(user = User("2", true, "FR")),
      base().copy(user = User("3", true, "FR"))
    )
    Main.withSession((spark) => {
      import spark.implicits._
      val res = spark
        .createDataset(data)
        .transform(Main.preprocess)
        .transform(Main.report)
        .cache()

      assert(res.count() == 1)
      assertFirstLineCol(res, "users", 3L)
    })
  }
  it should "compute event type metrics" in {
    val data = Seq(
      base().copy(`type` = "pageview"),
      base().copy(`type` = "pageview", user = base().user.copy(consent = true)),
      base().copy(`type` = "consent.asked"),
      base().copy(
        `type` = "consent.asked",
        user = base().user.copy(consent = true)
      ),
      base().copy(`type` = "consent.given"),
      base().copy(
        `type` = "consent.given",
        user = base().user.copy(consent = true)
      )
    )
    Main.withSession((spark) => {
      import spark.implicits._
      val res = spark
        .createDataset(data)
        .transform(Main.preprocess)
        .transform(Main.report)
        .cache()

      assert(res.count() == 1)
      assertFirstLineCol(res, "pageviews", 2L)
      assertFirstLineCol(res, "pageviews_with_consent", 1L)
      assertFirstLineCol(res, "consents_asked", 2L)
      assertFirstLineCol(res, "consents_asked_with_consent", 1L)
      assertFirstLineCol(res, "consents_given", 2L)
      assertFirstLineCol(res, "consents_given_with_consent", 1L)
    })
  }
}
