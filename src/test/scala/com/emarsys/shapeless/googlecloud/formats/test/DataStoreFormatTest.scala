package com.emarsys.shapeless.googlecloud.formats.test

import java.util.UUID
import com.emarsys.shapeless.googlecloud.formats.DataStore._
import com.emarsys.shapeless.googlecloud.formats.syntax._
import com.google.cloud.datastore.{Entity, Key, Value}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest._
import shapeless._

class DataStoreFormatTest extends WordSpec with Matchers {

  import TestTypes._


  "DataStore Entity" when {
    val key = Key.builder("test", "test", UUID.randomUUID().toString).build()

    "created from a case class" should {

      "be built from HNil" in {
        val event: HNil = HNil
        val entity = event.toEntity("project-id", "namespace", "kind")
        entity.hasKey shouldBe true
        entity.names shouldBe empty
      }

      "be built from the simplest case class" in {
        val event = TestInt(1)
        event.toEntity("project-id", "namespace", "kind").getLong("value").toInt shouldBe 1
      }

      "be built from the simple case class with string and int" in {
        val event = TestStringInt(s = "string", i = 1)
        val entity = event.toEntity("project-id", "namespace", "kind")
        entity.getLong("i").toInt shouldBe 1
        entity.getString("s") shouldBe "string"
      }

      "be built from simple case class with double value" in {
        val event = TestStringIntDouble(s = "simple", i = 1, d = 0.456789)
        val entity = event.toEntity("project-id", "namespace", "kind")
        entity.getLong("i").toInt shouldBe 1
        entity.getString("s") shouldBe "simple"
        entity.getDouble("d") shouldBe 0.456789
      }

      "be built from case class with boolean value" in {
        val trueEvent = TestBoolean(condition = true)
        val falseEvent = TestBoolean(condition = false)
        val trueEntity = trueEvent.toEntity("project-id", "namespace", "kind")
        val falseEntity = falseEvent.toEntity("project-id", "namespace", "kind")
        trueEntity.getBoolean("condition") shouldBe true
        falseEntity.getBoolean("condition") shouldBe false
      }

      "be built from case class with joda date value" in {
        val date: DateTime = new DateTime(2016, 11, 12, 13, 14, 50, DateTimeZone.UTC)
        val event = TestDateTime(date = date)
        val entity: Entity = event.toEntity("project-id", "namespace", "kind")
        entity.getValue("date").asInstanceOf[Value[Long]].parseAs[DateTime] shouldBe date
      }

      "be build with proper meta data" in {
        val entity = TestInt(1).toEntity("project-id", "namespace", "kind")
        entity.key.name.length shouldEqual 36
        entity.key.projectId() shouldEqual "project-id"
        entity.key.namespace() shouldEqual "namespace"
        entity.key.kind() shouldEqual "kind"
      }

    }

    "parsed to a case class" should {

      "be parsable to HNil" in {
        val entity = Entity.builder(key).build()
        entity.parseTo[HNil] shouldBe HNil
      }

      "be parsable to Simple case class" in {
        val entityBuilder = Entity.builder(key)
        entityBuilder.set("value", 1)
        val entity: Entity = entityBuilder.build()
        entity.parseTo[TestInt] shouldBe TestInt(1)
      }


    }

    "created and then parsed to a case class" should {

      "work with different field types" in {
        val event = TestStringIntDouble("simple", 1, 0.456789)
        val entity = event.toEntity("project-id", "namespace", "kind")
        entity.parseTo[TestStringIntDouble] shouldBe event
      }

      "be parsable partially to case class with default values" in {
        val entityBuilder = Entity.builder(key)
        entityBuilder.set("s", "stringy")
        val entity: Entity = entityBuilder.build()
        entity.parseTo[TestStringIntDouble] shouldBe TestStringIntDouble("stringy", 0, 0.0)
      }

    }

  }
}
