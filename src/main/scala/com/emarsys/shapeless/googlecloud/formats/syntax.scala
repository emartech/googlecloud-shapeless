package com.emarsys.shapeless.googlecloud.formats

import com.emarsys.shapeless.googlecloud.formats
import com.google.api.services.bigquery.model.TableRow
import com.google.datastore.v1.{Entity => EntityV1, Value => ValueV1}
import com.google.cloud.datastore.{Entity, Value}

object syntax {

  import formats._

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

  implicit class DataStoreV1Enricher[T](val t: T) {
    def toEntityV1(namespace: String, kind: String, keyFields: List[String])(implicit storable: DataStoreV1Format[T]): EntityV1 =
      storable.toEntity(namespace, kind, keyFields)(t)
  }

  implicit class EntityV1Parser(val entity: EntityV1) extends AnyVal {
    def parseTo[T](implicit dataEntityParsable: DataStoreV1Format[T]): T = dataEntityParsable.parseEntity(entity)
  }

  implicit class RichValueV1(val value: ValueV1) extends AnyVal {
    def parseAs[T](implicit valueFormat: EntityValueV1[T]): T = valueFormat.fromValue(value)
  }

  implicit class DataStoreEnricher[T](val t: T) {
    def toEntity(projectId: String, namespace: String, kind: String)(implicit storable: DataStoreFormat[T]): Entity =
      storable.toEntity(projectId, namespace, kind)(t)
  }

  implicit class EntityParser(val entity: Entity) extends AnyVal {
    def parseTo[T](implicit dataEntityParsable: DataStoreFormat[T]): T = dataEntityParsable.parseEntity(entity)
  }

  implicit class RichValue[B](val value: Value[B]) extends AnyVal {
    def parseAs[T](implicit valueFormat: EntityValue[T] { type ResultType = B }): T = valueFormat.fromValue(value)
  }

}
