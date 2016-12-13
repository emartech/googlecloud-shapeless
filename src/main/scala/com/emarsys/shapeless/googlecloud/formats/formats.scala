package com.emarsys.shapeless.googlecloud

import com.google.api.services.bigquery.model.TableRow
import com.google.datastore.v1.{Entity => EntityV1, Value => ValueV1}
import com.google.cloud.datastore.{Entity, Value}

package object formats {

  type FormatResult[T] = Either[String, T]

  trait BigQueryFormat[T] {
    def fromTableRow(tr: TableRow): FormatResult[T]
    def toTableRow(t: T): TableRow
  }

  trait BigQueryValue[T] {
    def toValue(v: T): AnyRef
    def fromValue(v: AnyRef): T
  }

  trait DataStoreV1Format[T] {
    def toEntity(namespace: String, kind: String, keyFields: List[String])(t: T): EntityV1
    def parseEntity(e: EntityV1): T
  }

  trait EntityValueV1[T] {
    val default : T
    def toValue(t: T): ValueV1
    def fromValue(v: ValueV1): T
  }

  trait DataStoreFormat[T] {
    def toEntity(projectId: String, namespace: String, kind: String)(t: T): Entity
    def parseEntity(e: Entity): T
  }

  trait EntityValue[T] {
    type ResultType
    val default : T
    def toValue(t: T): Value[ResultType]
    def fromValue(v: Value[ResultType]): T
  }

}




