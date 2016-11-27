
package com.emarsys.shapeless.googlecloud.formats

import java.util.UUID

import com.google.datastore.v1.{Entity, Value}
import com.google.datastore.v1.client.DatastoreHelper._
import org.joda.time.{DateTime, DateTimeZone}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Typeable, Witness}
import shapeless.labelled._

import scala.util.Try

object DataStore {

  implicit val intEntityValue = new EntityValue[Int] {
    override def toValue(i: Int): Value = makeValue(i.toLong).build()
    override def fromValue(v: Value): Int = v.getIntegerValue.toInt
  }

  implicit val stringEntityValue = new EntityValue[String] {
    override def toValue(s: String): Value = makeValue(s).build()
    override def fromValue(v: Value): String = v.getStringValue
  }

  implicit val doubleEntityValue = new EntityValue[Double] {
    override def toValue(d: Double): Value = makeValue(d).build()
    override def fromValue(v: Value): Double = v.getDoubleValue
  }

  implicit val booleanEntityValue = new EntityValue[Boolean] {
    override def toValue(t: Boolean): Value = makeValue(t).build()
    override def fromValue(v: Value): Boolean = v.getBooleanValue
  }

 implicit val DatePrimitive = new EntityValue[DateTime] {
   def toValue(dateTime: DateTime) = {
     makeValue(dateTime.toDateTime(DateTimeZone.UTC).getMillis * 10).build()
   }

   def fromValue(v: Value) = {
     val timestampPb = v.getIntegerValue
     val milliSeconds: Long = timestampPb / 10
     new org.joda.time.DateTime(milliSeconds, DateTimeZone.UTC)
   }
 }

  implicit def optionEntityValue[T](implicit innerValue: EntityValue[T]) = new EntityValue[Option[T]] {
      override def toValue(ot: Option[T]) = {
        ot match {
          case Some(x) => innerValue.toValue(x)
          case None    => null 
        }
      }

    override def fromValue(v: Value) = {
      if(v == null) None
      else Option(innerValue.fromValue(v))
      }
  }


  implicit val hNilStorable: DataStoreFormat[HNil] = new DataStoreFormat[HNil] {
    override def toEntity(namespace: String, kind: String, keyFields: List[String])(t: HNil): Entity =
      Entity.newBuilder().build()

    override def parseEntity(e: Entity): HNil = HNil
  }

  implicit def hListStorable[Key <: Symbol, V, Tail <: HList](implicit
    witness: Witness.Aux[Key],
    storeHead: Lazy[EntityValue[V]],
    storeRemaining: Lazy[DataStoreFormat[Tail]]): DataStoreFormat[FieldType[Key, V] :: Tail] =
    new DataStoreFormat[FieldType[Key, V] :: Tail] {

      override def toEntity(namespace: String, kind: String, keyFields: List[String])(t: FieldType[Key, V] :: Tail): Entity = {
        val rest = storeRemaining.value.toEntity(namespace, kind, keyFields)(t.tail)
        val headLabel = witness.value.name
        val headValue = storeHead.value.toValue(t.head)

        (buildEntity(headLabel) andThen
          mergeEntity(rest) andThen
          addOrAppendKeyToEntity(EntityOptions(namespace, kind, keyFields), headLabel, t.head)
        ) apply headValue
      }

      override def parseEntity(e: Entity): FieldType[Key, V] :: Tail = {
        val key = witness.value.name
        val value: Value = e.getProperties.get(key)
        val head: V = storeHead.value.fromValue(value)
        field[Key](head) :: storeRemaining.value.parseEntity(e)
      }
    }


  implicit def dataStoreFamilyFormat[T, Repr](
    implicit
    gen: LabelledGeneric.Aux[T, Repr],
    sg: Lazy[DataStoreFormat[Repr]],
    tpe: Typeable[T]
  ): DataStoreFormat[T] = new DataStoreFormat[T] {

    override def toEntity(namespace: String, kind: String, keyFields: List[String])(t: T) = {
      sg.value.toEntity(namespace, kind, keyFields)(gen.to(t))
    }

    override def parseEntity(e: Entity): T = {
      gen.from(sg.value.parseEntity(e))
    }
  }

  case class EntityOptions(namespace: String, kind: String, keyFields: List[String])

  private def buildEntity(label: String) = (value: Value) => {
    val entityBuilder = Entity.newBuilder()
    entityBuilder.getMutableProperties.put(label, value)
    entityBuilder.build()
  }

  private def mergeEntity(sourceEntity: Entity) = (entity: Entity) =>  sourceEntity.toBuilder.mergeFrom(entity).build()

  private def addOrAppendKeyToEntity[T](options: EntityOptions, fieldName: String, fieldValue: T) = (entity: Entity) => {
    if (options.keyFields.isEmpty) {
      addKeyToEntity(options, entity, UUID.randomUUID().toString)
    } else if (options.keyFields.contains(fieldName)) {
      val mergedKey = getCurrentKeyOfEntity(entity).fold(fieldValue.toString)(fieldValue + "_" + _)
      addKeyToEntity(options, entity, mergedKey)
    } else {
      entity
    }
  }

  private def getCurrentKeyOfEntity(entity: Entity): Option[String] = {
    if (entity.getKey.getPathCount > 0) Some(entity.getKey.getPath(0).getName) else None
  }

  private def addKeyToEntity(options: EntityOptions, entity: Entity, key: String) = {
    val keyBuilder = makeKey(options.kind, key)
    if (options.namespace != null) {
      keyBuilder.getPartitionIdBuilder.setNamespaceId(options.namespace)
    }
    entity.toBuilder.setKey(keyBuilder.build()).build()
  }
}
