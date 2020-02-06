package com.didomi.challenge.data
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.UUID

case class User(
    id: String,
    consent: Boolean,
    country: String
)

case class Event(
    timestamp: Timestamp,
    id: String,
    `type`: String,
    domain: String,
    user: User
)

object Event {
  def mock(etype: String, user: User): Event = Event(
    Timestamp.valueOf(LocalDateTime.now()),
    UUID.randomUUID().toString(),
    etype,
    "www.website.com",
    user
  )
}
