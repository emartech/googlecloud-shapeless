package com.emarsys.shapeless.googlecloud.formats

import com.google.api.client.util.Data._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.Try
import shapeless._
import labelled.{FieldType, field}
import com.google.api.services.bigquery.model.{TableCell, TableRow}
import org.joda.time.{DateTime, DateTimeZone}

object BigQuery {

  implicit object stringPrimitive extends BigQueryValue[String] {
    def toValue(s: String) = s
    def fromValue(v: AnyRef) = v.toString
  }

  implicit object intPrimitive extends BigQueryValue[Int] {
    def toValue(s: Int) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toInt
  }

  implicit object DoublePrimitive extends BigQueryValue[Double] {
    def toValue(s: Double) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toDouble
  }

  implicit object FloatPrimitive extends BigQueryValue[Float] {
    def toValue(s: Float) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toFloat
  }

  implicit object BoolPrimitive extends BigQueryValue[Boolean] {
    def toValue(s: Boolean) =
      s.asInstanceOf[AnyRef]
    def fromValue(v: AnyRef) =
      v.toString.toBoolean
  }

  implicit object DatePrimitive extends BigQueryValue[DateTime] {
    def toValue(s: DateTime) = {
      (s.toDateTime(DateTimeZone.UTC).getMillis / 1000).toString.asInstanceOf[AnyRef]
    }

    def fromValue(v: AnyRef) = {
      val d = v.toString.toDouble
      val l = (d * 1000).toLong
      new org.joda.time.DateTime(l, DateTimeZone.UTC)
    }
  }

  implicit def optionType[T](implicit tType: BigQueryValue[T]): BigQueryValue[Option[T]] =
    new BigQueryValue[Option[T]] {
      override def toValue(v: Option[T]): AnyRef = (v map tType.toValue).orNull

      override def fromValue(v: AnyRef): Option[T] = if (isNull(v)) None else {
        Try(tType.fromValue(v)).toOption
      }
    }

  implicit def optionFormat[T](implicit tFormat: BigQueryFormat[T]): BigQueryFormat[Option[T]] =
    new BigQueryFormat[Option[T]] {
      override def fromTableRow(tr: TableRow): FormatResult[Option[T]] = {
        Right(tFormat.fromTableRow(tr).right.toOption)
      }

      override def toTableRow(t: Option[T]): TableRow = {
        (t map tFormat.toTableRow).get
      }
    }

  implicit object hNilBigQueryFormat extends BigQueryFormat[HNil] {
    def toTableRow(t: HNil) = {
      val tr = new TableRow()
      tr.setF(List[TableCell]().asJava)
    }
    def fromTableRow(m: TableRow) =
      Right(HNil)
  }

  implicit def hListBigQueryFormat[Key <: Symbol, V, Tail <: HList](
    implicit
    witness: Witness.Aux[Key],
    valueFormatter: Lazy[BigQueryValue[V]],
    restFormatter: Lazy[BigQueryFormat[Tail]]
  ): BigQueryFormat[FieldType[Key, V] :: Tail] =
    new BigQueryFormat[FieldType[Key, V] :: Tail] {
      def toTableRow(t: FieldType[Key, V] :: Tail): TableRow = {
        val head = valueFormatter.value.toValue(t.head)
        val c = new TableCell
        c.setV(head)
        val rest = restFormatter.value.toTableRow(t.tail)
        val f = rest.getF.toList
        rest.setF((c :: f).asJava)

      }

      def fromTableRow(m: TableRow): FormatResult[FieldType[Key, V] :: Tail] = {
        val f: List[TableCell] = m.getF.toList
        val v: AnyRef = f.head.getV
        val x = valueFormatter.value.fromValue(v)
        val h = field[Key](x)
        m.setF(f.tail.asJava)
        val tail = restFormatter.value.fromTableRow(m)
        tail match {
          case Left(e) => Left(e)
          case Right(t) =>
            Right(h :: t)
        }

      }
    }

  implicit val cNilBigQueryFormat = new BigQueryFormat[CNil] {
    def toTableRow(t: CNil): TableRow =
      new TableRow
    def fromTableRow(m: TableRow) =
      Left("CNil")
  }

  implicit def coproductBigQueryFormat[Key <: Symbol, V, Rest <: Coproduct](
    implicit
    witness: Witness.Aux[Key],
    valueFormatter: Lazy[BigQueryFormat[V]],
    restFormatter: Lazy[BigQueryFormat[Rest]]
  ): BigQueryFormat[FieldType[Key, V] :+: Rest] =
    new BigQueryFormat[FieldType[Key, V] :+: Rest] {
      def toTableRow(t: FieldType[Key, V] :+: Rest) = {
        t match {
          case Inl(h) =>
            val r = valueFormatter.value.toTableRow(h)
            val typeCell = new TableCell
            typeCell.setV(witness.value.name)
            val augmentedFields = typeCell :: r.getF.toList
            r.setF(augmentedFields)
          case Inr(r) =>
            restFormatter.value.toTableRow(r)
        }
      }

      def fromTableRow(m: TableRow) = {
        val thisType = m.getF.headOption.map(_.getV).contains(witness.value.name)
        if (thisType) {
          val rowWithoutType = new TableRow()
          rowWithoutType.setF(m.getF.tail.toList.asJava)
          valueFormatter.value.fromTableRow(rowWithoutType) match {
            case Right(x) =>
              Right(Inl(field[Key](x)))
            case Left(y) =>
              Left(y)
          }
        } else {
          restFormatter.value.fromTableRow(m) match {
            case Right(x) =>
              Right(Inr(x))
            case Left(y) =>
              Left(y)
          }
        }
      }
    }

  implicit def familyBigQueryFormat[T, Repr](implicit
    gen: LabelledGeneric.Aux[T, Repr],
    reprFormatter: Lazy[BigQueryFormat[Repr]],
    tpe: Typeable[T]): BigQueryFormat[T] =
    new BigQueryFormat[T] {
      def toTableRow(t: T) =
        reprFormatter.value.toTableRow(gen.to(t))
      def fromTableRow(m: TableRow) =
        try {
          reprFormatter.value.fromTableRow(m) match {
            case Right(x) =>
              Right(gen.from(x))
            case Left(y) => Left(y)
          }
        } catch {
          case e: Throwable =>
            Left("Format failed: " + e.getMessage)
        }
    }
}

