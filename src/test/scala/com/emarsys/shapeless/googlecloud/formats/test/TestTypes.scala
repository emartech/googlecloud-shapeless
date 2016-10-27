package com.emarsys.shapeless.googlecloud.formats.test

import org.joda.time.DateTime

case class Person(name: String, age: Int)

case class MaybePerson(name: String, age: Option[Int])

case class Embedded(p: MaybePerson, level: Int)
case class Simple(i: Int)
case class SimpleStringy(s: String, i: Int)
case class SimpleStringyDouble(s: String, i: Int, d: Double)

object testTypes {

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