package com.emarsys.shapeless.googlecloud.formats

import com.google.cloud.datastore.{BooleanValue, _}
import org.joda.time.{DateTime, DateTimeZone}
import shapeless.labelled._
import shapeless.{::, HList, HNil, LabelledGeneric, Lazy, Typeable, Witness}
import java.util.UUID
import scala.util.Try

object DataStore {

  case class EntityOptions(projectId: String, namespace: String, kind: String)

  implicit val intEntityValue = new EntityValue[Int] {
    override type ResultType = Long
    override val default = 0
    override def toValue(i: Int): Value[Long] = LongValue.builder(i.toLong).build().asInstanceOf[Value[Long]]
    override def fromValue(v: Value[Long])    = v.get.toInt
  }

  implicit val stringEntityValue = new EntityValue[String] {
    override type ResultType = String
    override val default = ""
    override def toValue(s: String): Value[String]   = StringValue.builder(s).build().asInstanceOf[Value[String]]
    override def fromValue(v: Value[String]): String = v.get
  }

  implicit val doubleEntityValue = new EntityValue[Double] {
    override type ResultType = Double
    override val default = 0d
    override def toValue(d: Double): Value[Double]   = DoubleValue.builder(d).build().asInstanceOf[Value[Double]]
    override def fromValue(v: Value[Double]): Double = v.get
  }

  implicit val booleanEntityValue = new EntityValue[Boolean] {
    override type ResultType = Boolean
    override val default = false
    override def toValue(t: Boolean): Value[Boolean]   = BooleanValue.builder(t).build().asInstanceOf[Value[Boolean]]
    override def fromValue(v: Value[Boolean]): Boolean = v.get
  }

  implicit val DatePrimitive = new EntityValue[DateTime] {
    override type ResultType = Long
    override val default = new DateTime(0)
    def toValue(dateTime: DateTime): Value[Long] = {
      LongValue.builder(dateTime.toDateTime(DateTimeZone.UTC).getMillis * 10).build().asInstanceOf[Value[Long]]
    }

    def fromValue(v: Value[Long]) = {
      val timestampPb  = v.get
      val milliSeconds = timestampPb / 10
      new org.joda.time.DateTime(milliSeconds, DateTimeZone.UTC)
    }
  }

  implicit val hNilStorable: DataStoreFormat[HNil] = new DataStoreFormat[HNil] {
    override def toEntity(projectId: String, namespace: String, kind: String)(t: HNil): Entity = {
      Entity.builder(buildKey(EntityOptions(projectId, namespace, kind))).build()
    }

    override def parseEntity(e: Entity): HNil = HNil
  }

  implicit def hListStorable[Key <: Symbol, V, Tail <: HList](
      implicit witness: Witness.Aux[Key],
      storeHead: Lazy[EntityValue[V]],
      storeRemaining: Lazy[DataStoreFormat[Tail]]): DataStoreFormat[FieldType[Key, V] :: Tail] = {

    new DataStoreFormat[FieldType[Key, V] :: Tail] {

      override def toEntity(projectId: String, namespace: String, kind: String)(
          t: FieldType[Key, V] :: Tail): Entity = {
        val rest      = storeRemaining.value.toEntity(projectId, namespace, kind)(t.tail)
        val headLabel = witness.value.name
        val headValue = storeHead.value.toValue(t.head)

        buildEntity(EntityOptions(projectId, namespace, kind), headLabel, headValue, rest)
      }

      override def parseEntity(e: Entity): FieldType[Key, V] :: Tail = {
        val key                                      = witness.value.name
        val head: V                               =
           Try(storeHead.value.fromValue(e.getValue(key)))
          .getOrElse(storeHead.value.default)
        field[Key](head) :: storeRemaining.value.parseEntity(e)
      }
    }
  }

  implicit def dataStoreFamilyFormat[T, Repr](implicit gen: LabelledGeneric.Aux[T, Repr],
                                              sg: Lazy[DataStoreFormat[Repr]],
                                              tpe: Typeable[T]): DataStoreFormat[T] = new DataStoreFormat[T] {

    override def toEntity(projectId: String, namespace: String, kind: String)(t: T) = {
      sg.value.toEntity(projectId: String, namespace, kind)(gen.to(t))
    }

    override def parseEntity(e: Entity): T = {
      gen.from(sg.value.parseEntity(e))
    }
  }

  private def buildEntity[T](options: EntityOptions, label: String, value: Value[T], sourceEntity: Entity) = {
    val entityBuilder = Entity.builder(sourceEntity).key(buildKey(options)).set(label, value)
    entityBuilder.build()
  }

  def buildKey(options: EntityOptions) =
    Key.builder(options.projectId, options.kind, UUID.randomUUID().toString).namespace(options.namespace).build()

}
