package com.emarsys.shapeless.googlecloud.formats

import com.emarsys.shapeless.googlecloud.formats
import com.google.api.services.bigquery.model.TableRow
import com.google.datastore.v1.{Entity, Value}
import com.google.protobuf.Timestamp
import org.joda.time.DateTime

object syntax {
  import formats._

  implicit class DataStoreEnricher[T](val t: T) {
    def toEntity(namespace: String, kind: String, keyFields: List[String])(implicit storable: DataStoreFormat[T]): Entity =
      storable.toEntity(namespace, kind, keyFields)(t)
  }

  implicit class RichBigResult[R](val e: FormatResult[R]) extends AnyVal {
    def getOrThrowError: R = e match {
      case Left(error) =>
        throw new IllegalArgumentException(error)
      case Right(r) => r
    }
  }

  implicit class RichBigDataFormat[T](val t: T) extends AnyVal {
    def toTableRow(implicit s: BigQueryFormat[T]): TableRow = s.toTableRow(t)

  }

  implicit class RichProperties(val row: TableRow) extends AnyVal {
    def as[T](implicit s: BigQueryFormat[T]): T =
      s.fromTableRow(row).getOrThrowError

    def to[T](implicit t: BigQueryValue[T]): T =
      t.fromValue(row.getF.get(0).getV)
  }

  implicit class EntityParser(val entity: Entity) extends AnyVal {
    def parseTo[T](implicit dataEntityParsable: DataStoreFormat[T]): T = dataEntityParsable.parseEntity(entity)
  }

  implicit class RichValue(val value: Value) extends AnyVal {
    def parseAs[T](implicit valueFormat: EntityValue[T]) : T = valueFormat.fromValue(value)
  }
}
