package com.emarsys.shapeless.googlecloud.formats.test

import com.emarsys.shapeless.googlecloud.formats.DataStore._
import com.emarsys.shapeless.googlecloud.formats.syntax._
import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest._
import shapeless._

class DataStoreFormatTest extends FlatSpec with Matchers {

  import TestTypes._

  "Entity" should "be built from HNil" in {
    val event: HNil = HNil
    event.toEntity("test", "test", List()) shouldBe Entity.newBuilder().build()
  }

  "Entity" should "be built from the simplest case class" in {
    val event = TestInt(1)
    event.toEntity("test", "test", List()).getProperties.get("value").getIntegerValue shouldBe 1
  }

  "Entity" should "be built from the simple case class with string and int" in {
    val event = TestStringInt("string", 1)
    val entity = event.toEntity("test", "test", List())
    entity.getProperties.get("i").getIntegerValue shouldBe 1
    entity.getProperties.get("s").getStringValue shouldBe "string"
  }

  "Entity" should "be built from simple case class with double value" in {
    val event = TestStringIntDouble("simple", 1, 0.456789)
    val entity = event.toEntity("test", "test", List())
    entity.getProperties.get("i").getIntegerValue shouldBe 1
    entity.getProperties.get("s").getStringValue shouldBe "simple"
    entity.getProperties.get("d").getDoubleValue shouldBe 0.456789
  }

  "Entity" should "be built from case class with boolean value" in {
    val trueEvent = TestBoolean(true)
    val falseEvent = TestBoolean(false)
    val trueEntity = trueEvent.toEntity("test", "test", List())
    val falseEntity = falseEvent.toEntity("test","test", List())
    trueEntity.getProperties.get("condition").getBooleanValue shouldBe true
    falseEntity.getProperties.get("condition").getBooleanValue shouldBe false
  }

  "Entity" should "be built from case class with joda date value" in {
    val date: DateTime = new DateTime(2016, 11, 12, 13, 14, 50, DateTimeZone.UTC)
    val event = TestDateTime(date)
    val entity: Entity = event.toEntity("test", "test", List())
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
    entityBuilder.getMutableProperties.put("value", DatastoreHelper.makeValue(1).build())
    val entity: Entity = entityBuilder.build()
    entity.parseTo[TestInt] shouldBe TestInt(1)
  }

  "Simple case class with different fields" should "roundtrip" in {
    val event = TestStringIntDouble("simple", 1, 0.456789)
    val entity = event.toEntity("test", "test", List())
    entity.parseTo[TestStringIntDouble] shouldBe event
  }

  "case class with optional values" should "roundtrip" in {
    val event = TestOptional(None, Some("some"), None)
    val entity = event.toEntity("test", "test", List())
    entity.parseTo[TestOptional] shouldBe event
  }

  "Entity" should "have uuid key if no key field is defined" in {
    val event = TestInt(1)
    event.toEntity("test", "test", List()).getKey.getPath(0).getName.length shouldEqual 36
  }

  "Entity" should "be built with first field as key" in {
    val event = TestInt(1)
    event.toEntity("test", "test", List("value")).getKey.getPath(0).getName shouldEqual "1"
  }

  "Entity" should "be built with any field as key" in {
    val event = TestStringInt("not key", 1)
    event.toEntity("test", "test", List("i")).getKey.getPath(0).getName shouldEqual "1"
  }

  "Entity" should "be built with any fields as composit key" in {
    val event = TestStringIntDouble("not key", 1, 0.1)
    event.toEntity("test", "test", List("s", "i")).getKey.getPath(0).getName shouldEqual "not key_1"
  }
}
