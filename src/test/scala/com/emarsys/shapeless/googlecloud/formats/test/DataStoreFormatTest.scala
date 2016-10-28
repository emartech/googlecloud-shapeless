package com.emarsys.shapeless.googlecloud.formats.test

import com.emarsys.shapeless.googlecloud.formats.DataStore._
import com.emarsys.shapeless.googlecloud.formats.syntax._
import com.google.datastore.v1.Entity
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest._
import shapeless._

class DataStoreFormatTest extends FlatSpec with Matchers {
  case class TestBoolean(condition: Boolean)
  case class TestDateTime(date: DateTime)

  "Entity" should "be built from HNil" in {
    val event: HNil = HNil
    event.toEntity("test", "test") shouldBe Entity.newBuilder().build()
  }

  "Entity" should "be built from the simplest case class" in {
    val event = Simple(1)
    event.toEntity("test", "test").getProperties.get("i").getIntegerValue shouldBe 1
  }

  "Entity" should "be built from the simple case class with string and int" in {
    val event = SimpleStringy("string", 1)
    val entity = event.toEntity("test", "test")
    entity.getProperties.get("i").getIntegerValue shouldBe 1
    entity.getProperties.get("s").getStringValue shouldBe "string"
  }

  "Entity" should "be built from simple case class with double value" in {
    val event = SimpleStringyDouble("simple", 1, 0.456789)
    val entity = event.toEntity("test", "test")
    entity.getProperties.get("i").getIntegerValue shouldBe 1
    entity.getProperties.get("s").getStringValue shouldBe "simple"
    entity.getProperties.get("d").getDoubleValue shouldBe 0.456789
  }

  "Entity" should "be built from case class with boolean value" in {
    val trueEvent = TestBoolean(true)
    val falseEvent = TestBoolean(false)
    val trueEntity = trueEvent.toEntity("test", "test")
    val falseEntity = falseEvent.toEntity("test","test")
    trueEntity.getProperties.get("condition").getBooleanValue shouldBe true
    falseEntity.getProperties.get("condition").getBooleanValue shouldBe false
  }

  "Entity" should "be built from case class with joda date value" in {
    val date: DateTime = new DateTime(2016, 11, 12, 13, 14, 50, DateTimeZone.UTC)
    val event = TestDateTime(date)
    val entity: Entity = event.toEntity("test", "test")
    val timestampPb = entity.getProperties.get("date").getTimestampValue
    val milliSeconds: Long = timestampPb.getSeconds * 1000
    entity.getProperties.get("date").parseAs[DateTime] shouldBe date
  }


  "Entity" should "be parsable to HNil" in {
    val entity = Entity.newBuilder().build()
    entity.parseTo[HNil] shouldBe HNil
  }

  "Entity" should "be parsable to Simple case class" in {
    val entityBuilder = Entity.newBuilder()
    entityBuilder.getMutableProperties.put("i", com.google.datastore.v1.client.DatastoreHelper.makeValue(1).build())
    val entity: Entity = entityBuilder.build()
    entity.parseTo[Simple] shouldBe Simple(1)
  }

  "Simple case class with different fields" should "roundtrip" in {
    val event = SimpleStringyDouble("simple", 1, 0.456789)
    val entity = event.toEntity("test", "test")
    entity.parseTo[SimpleStringyDouble] shouldBe event
  }
}
