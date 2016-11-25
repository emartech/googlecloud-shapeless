package com.emarsys.shapeless.googlecloud

import com.google.api.services.bigquery.model.TableRow
import com.google.datastore.v1.{Entity, Value}

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

  trait DataStoreFormat[T] {
    def toEntity(namespace: String, kind: String, keyFields: List[String])(t: T): Entity
    def parseEntity(e: Entity) : T
  }

  trait EntityValue[T] {
    def toValue(t: T): Value
    def fromValue(v: Value) : T
  }
}




