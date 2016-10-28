package com.emarsys.shapeless.googlecloud.formats.test

import com.emarsys.shapeless.googlecloud.formats.BigQuery._
import com.emarsys.shapeless.googlecloud.formats.syntax._
import com.google.api.services.bigquery.model.{TableCell, TableRow}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class BigQueryFormatTest extends WordSpec with Matchers {

  val testTime = new DateTime(2016, 11, 12, 13, 14, 50, DateTimeZone.UTC)


  object LocalTestTypes {

    sealed trait TestTrait

    case class TestString(name: String) extends TestTrait

    case class TestInt(value: Int) extends TestTrait

    case class TestDouble(value: Double) extends TestTrait

    case class TestFloat(value: Float) extends TestTrait

    case class TestBoolean(condition: Boolean) extends TestTrait

    case class TestJodaDateTime(dateTime: DateTime) extends TestTrait

    case class TestDateTime(date: DateTime) extends TestTrait

    case class TestMultipleTypes(name: String, condition: Boolean, dateTime: DateTime) extends TestTrait

  }

  import LocalTestTypes._

  "format" should {
    "round trip" in {
      val p = Person("John", 45)
      p.toTableRow.as[Person] shouldEqual p
    }

    "round trip with None" in {
      val p = MaybePerson("Paul", None)
      p.toTableRow.as[MaybePerson] shouldEqual p
    }

    "round trip with some value" in {
      val p = MaybePerson("Ringo", Some(65))
      p.toTableRow.as[MaybePerson] shouldEqual p
    }

    "round trip with date value, utc without milliseconds conforming to bigquery timestamps" in {
      val testDate = TestDateTime(testTime)
      testDate.toTableRow.as[TestDateTime] shouldEqual testDate
    }

    "create None from null value in table" in {
      val tr = new TableRow()
      val c  = new TableCell()
      val c2 = new TableCell()
      c2.put("v", null)
      c.put("v", "George")
      tr.put("f", List(c, c2).asJava)
      tr.as[MaybePerson] shouldEqual MaybePerson("George", None)
    }

    "cannot create None from missing value in table because the table does not contain field labels" in {
      val tr = new TableRow()
      val c  = new TableCell()
      c.put("v", "George")
      tr.put("f", List(c).asJava)
      an[java.lang.IllegalArgumentException] should be thrownBy tr.as[MaybePerson]
    }

    "round trip algebraic data type" which {

      "has a string field" in {
        val ts: TestTrait = TestString("TestString")
        val ti: TestTrait = TestInt(3)
        ts.toTableRow.as[TestTrait] shouldEqual ts
        ts.toTableRow.as[TestTrait] shouldNot be(ti)
      }

      "has an int field" in {
        val ti: TestTrait = TestInt(3)
        ti.toTableRow.as[TestTrait] shouldEqual ti
      }

      "has a double field" in {
        val td: TestTrait = TestDouble(3.14)
        td.toTableRow.as[TestTrait] shouldEqual td
      }

      "has a float field" in {
        val td: TestTrait = TestFloat(3.14f)
        td.toTableRow.as[TestTrait] shouldEqual td
      }

      "has a boolean field" in {
        val testBoolean: TestTrait = TestBoolean(true)
        testBoolean.toTableRow.as[TestTrait] shouldEqual testBoolean
      }

      "has a date time field" in {
        val testdateTime: TestTrait = TestDateTime(testTime)
        testdateTime.toTableRow.as[TestTrait] shouldEqual testdateTime
      }

      "has multiple field types" in {
        val testMultipleTypes: TestTrait = TestMultipleTypes("TestString", condition = true, testTime)
        testMultipleTypes.toTableRow.as[TestTrait] shouldEqual testMultipleTypes
      }

    }

    "round trip match to concrete type of ADT" which {
      "string" in {
        val testString: TestTrait = TestString("Test string")
        testString.toTableRow.as[TestTrait] match {
          case TestString(name) => name shouldEqual "Test string"
          case _                => fail
        }

      }

      "Int" in {
        val testString: TestTrait = TestInt(3)
        testString.toTableRow.as[TestTrait] match {
          case TestInt(v) => v shouldEqual 3
          case _          => fail
        }

      }

      "Double" in {
        val testString: TestTrait = TestDouble(3.0)
        testString.toTableRow.as[TestTrait] match {
          case TestDouble(v) => v shouldEqual 3.0
          case _             => fail
        }

      }

      "Float" in {
        val testString: TestTrait = TestFloat(3.0f)
        testString.toTableRow.as[TestTrait] match {
          case TestFloat(v) => v shouldEqual 3.0f
          case _            => fail
        }

      }

      "Date" in {
        val testString: TestTrait = TestDateTime(testTime)
        testString.toTableRow.as[TestTrait] match {
          case TestDateTime(date) => date shouldEqual testTime
          case _                  => fail
        }

      }
    }

    "algebraic data type starts with a 'type' field in TableRow with the name of the case class" in {
      val ts: TestTrait = TestString("TestString")
      val ti: TestTrait = TestInt(3)
      val td: TestTrait = TestDouble(3.14)
      val tf: TestTrait = TestFloat(3.14f)

      def tester(t: TestTrait, expectedName: String): Unit = {
        t.toTableRow.getF.get(0).getV shouldEqual expectedName
      }

      tester(td, "TestDouble")
      tester(ts, "TestString")
      tester(ti, "TestInt")
      tester(tf, "TestFloat")
    }

    "read single element table to primitive type" which {
      "is String" in {
        val row = new TableRow
        val cell = new TableCell
        cell.setV("Hello")
        row.setF(List(cell).asJava)

        row.to[String] shouldEqual "Hello"
      }

      "is Int" in {
        val row = new TableRow
        val cell = new TableCell
        cell.setV(1)
        row.setF(List(cell).asJava)

        row.to[Int] shouldEqual 1
      }

      "is Date from timestamp" in {
        val row = new TableRow
        val cell = new TableCell
        val expectedDate = new DateTime(2016, 11, 12, 13, 14, 50, DateTimeZone.UTC)
        val timestamp = (expectedDate.getMillis / 1000).toDouble
        cell.setV(timestamp)
        row.setF(List(cell).asJava)

        row.to[DateTime] shouldEqual expectedDate
      }
    }

    //@TODO: support embedded types
//    "embedded case class serialization" in {
//      val e = Embedded(Master("John", Some(45)), Detail(2))
//      e.toTableRow.as[Embedded] shouldEqual e
//    }
  }

}
