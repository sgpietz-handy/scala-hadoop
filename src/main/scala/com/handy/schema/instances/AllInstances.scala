package com.handy.schema

import org.apache.spark.sql.types

import atto._, Atto._, compat.stdlib._

import io.circe.Json
import io.circe.syntax._

import cats.implicits._

import com.handy.schema.hive.HiveSchema
import com.handy.schema.hive.HiveSchema._

trait AllInstances {
  implicit val schemaTypeToSchema = new Schema[SchemaType] {
    def apply(st: SchemaType): Either[Throwable, SchemaType] =
      Right(st)
  }

  implicit val dataTypeToSchema = new Schema[types.DataType] {
    def apply(dt: types.DataType): Either[Throwable, SchemaType] =
      dt match {
        case types.ByteType => Right(ByteType)
        case types.ShortType => Right(ShortType)
        case types.IntegerType => Right(IntType)
        case types.LongType => Right(LongType)
        case types.FloatType => Right(FloatType)
        case types.DoubleType => Right(DoubleType)
        case _: types.DecimalType => Right(DecimalType)
        case types.StringType => Right(StringType)
        case types.BooleanType => Right(BooleanType)
        case types.NullType => Right(NullType)

        case types.ArrayType(t, _) => apply(t).map(ArrayType)

        case types.MapType(keyType, valueType, _) => for {
          key <- apply(keyType).right
          primitiveKey <- (key match {
            case k: PrimitiveType => Right(k)
            case _ => Left(SchemaParseError("key of a map must be a primitive type"))
          }).right
          value <- apply(valueType).right
        }  yield MapType(primitiveKey, value)

        case types.StructType(fields) =>
          fields.toList
            .traverseU(f => apply(f.dataType).map(t => SchemaField(f.name, t, f.nullable)))
            .map(StructType)

        case _ => Left(SchemaParseError(s"unsupported spark data type $dt"))
      }
  }

  implicit val hiveToSchema =
    new Schema[HiveSchema] {
      def apply(schema: HiveSchema): Either[SchemaParseError, SchemaType] =
        schemaParser.parseOnly(schema.schema)
          .either
          .fold(msg => Left(SchemaParseError(msg)), st => Right(st))
    }

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
