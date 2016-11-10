package com.handy.schema

import io.circe.Json
import io.circe.syntax._

import com.handy.schema.hive._

package object json {
  implicit val jsonConvertsFrom = new ConvertsFrom[Json] {
    def toByte(a: Json): Option[Byte] = a.as[Byte].toOption
    def toShort(a: Json): Option[Short] = a.as[Short].toOption
    def toInt(a: Json): Option[Int] = a.as[Int].toOption
    def toLong(a: Json): Option[Long] = a.as[Long].toOption
    def toFloat(a: Json): Option[Float] = a.as[Float].toOption
    def toDouble(a: Json): Option[Double] = a.as[Double].toOption
    def toDecimal(a: Json): Option[BigDecimal] = a.as[BigDecimal].toOption
    def toString(a: Json): Option[String] = a.as[String].toOption.orElse(Some(a.toString))
    def toBinary(a: Json): Option[Array[Byte]] = toString(a).map(_.getBytes)
    def toBoolean(a: Json): Option[Boolean] = a.as[Boolean].toOption

    def toArray(a: Json): Option[List[Json]] = a.asArray
    def toMap(a: Json): Option[Map[Json, Json]] = toStruct(a).map(_.map {
      case (k, v) => (k.asJson, v)
    })
    def toStruct(a: Json): Option[Map[String, Json]] = a.asObject.map(_.toMap)

    def isNull(a: Json): Boolean = a.isNull
  }

  implicit val jsonConvertsTo = new ConvertsTo[Json] {
    def fromByte(a: Byte): Json = a.asJson
    def fromShort(a: Short) : Json = a.asJson
    def fromInt(a: Int): Json = a.asJson
    def fromLong(a: Long): Json = a.asJson
    def fromFloat(a: Float): Json = a.asJson
    def fromDouble(a: Double): Json = a.asJson
    def fromDecimal(a: BigDecimal): Json = a.asJson
    def fromString(a: String): Json = a.asJson
    def fromBinary(a: Array[Byte]): Json = a.asJson
    def fromBoolean(a: Boolean): Json = a.asJson
    def fromNull: Json = None.asJson

    def fromArray(a: List[Json]): Json = a.asJson
    def fromMap(a: Map[Json, Json]): Json = a.map(kv => (kv._1.toString, kv._2)).asJson
    def fromStruct(a: Map[String, Json]): Json = a.asJson
  }
}
