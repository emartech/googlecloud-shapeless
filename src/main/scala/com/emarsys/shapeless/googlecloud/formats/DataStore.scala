package com.emarsys.shapeless.googlecloud.formats

import java.util.UUID

import com.google.datastore.v1.{Entity, Value}
import com.google.datastore.v1.client.DatastoreHelper._
import org.joda.time.{DateTime, DateTimeZone}
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Typeable, Witness}
import shapeless.labelled._

object DataStore {

  implicit val intEntityValue = new EntityValue[Int] {
    override def toValue(i: Int): Value = makeValue(i).build()
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


  implicit val hNilStorable: DataStoreFormat[HNil] = new DataStoreFormat[HNil] {
    override def toEntity(namespace: String, kind: String)(t: HNil): Entity =
      Entity.newBuilder().build()

    override def parseEntity(e: Entity): HNil = HNil
  }

  implicit def hListStorable[Key <: Symbol, V, Tail <: HList](implicit
    witness: Witness.Aux[Key],
    storeHead: Lazy[EntityValue[V]],
    storeRemaining: Lazy[DataStoreFormat[Tail]]): DataStoreFormat[FieldType[Key, V] :: Tail] =
    new DataStoreFormat[FieldType[Key, V] :: Tail] {
      override def toEntity(namespace: String, kind: String)(t: FieldType[Key, V] :: Tail): Entity = {
        val rest = storeRemaining.value.toEntity(namespace: String, kind: String)(t.tail)
        val headLabel = witness.value.name
        val headValue = storeHead.value.toValue(t.head)
        val headEntity: Entity = buildEntity(namespace, kind, headLabel, headValue)
        val completeEntity: Entity = rest.toBuilder.mergeFrom(headEntity).build()
        completeEntity
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

    override def toEntity(namespace: String, kind: String)(t: T) = {
      sg.value.toEntity(namespace: String, kind: String)(gen.to(t))
    }

    override def parseEntity(e: Entity): T = {
      gen.from(sg.value.parseEntity(e))
    }
  }

  private def createKey(namespace: String, kind: String) = {
    def makeAncestorKey(namespace: String, kind: String) = {
      val keyBuilder = makeKey(kind, "root")
      if (namespace != null) {
        keyBuilder.getPartitionIdBuilder.setNamespaceId(namespace)
      }
      keyBuilder.build()
    }

    val ancestorKey = makeAncestorKey(namespace, kind)
    val keyBuilder = makeKey(ancestorKey, kind, UUID.randomUUID().toString)
    keyBuilder.getPartitionIdBuilder.setNamespaceId(namespace)
    keyBuilder
  }

  private def buildEntity(namespace: String, kind: String, label: String, value: Value): Entity = {
    val entityBuilder = Entity.newBuilder()
    entityBuilder.setKey(createKey(namespace, kind).build())
    entityBuilder.getMutableProperties.put(label, value)
    val headEntity: Entity = entityBuilder.build()
    headEntity
  }

}
