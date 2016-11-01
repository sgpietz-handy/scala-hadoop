package com.handy.schema

import io.circe.{Json, JsonObject}
import io.circe.syntax._

import cats.implicits._
import cats.data.Validated
import cats.data.Validated._

sealed trait SchemaType { val nullable: Boolean }
sealed trait PrimitiveType extends SchemaType
sealed trait ComplexType extends SchemaType
case class ByteType(nullable: Boolean) extends PrimitiveType
case class ShortType(nullable: Boolean) extends PrimitiveType
case class IntType(nullable: Boolean) extends PrimitiveType
case class LongType(nullable: Boolean) extends PrimitiveType
case class FloatType(nullable: Boolean) extends PrimitiveType
case class DoubleType(nullable: Boolean) extends PrimitiveType
case class DecimalType(nullable: Boolean) extends PrimitiveType
case class StringType(nullable: Boolean) extends PrimitiveType
case class BinaryType(nullable: Boolean) extends PrimitiveType
case class BooleanType(nullable: Boolean) extends PrimitiveType
case object NullType extends PrimitiveType { val nullable = true }
case class ArrayType(t: SchemaType, nullable: Boolean) extends ComplexType
case class MapType(k: PrimitiveType, v: SchemaType, nullable: Boolean) extends ComplexType
case class UnionType(ts: List[SchemaType], nullable: Boolean) extends ComplexType
case class StructType(fields: List[(String, SchemaType)], nullable: Boolean) extends ComplexType

object SchemaType {
  def convertWithSchema[A, B, S](a: A, schema: S)
  (implicit tv: ToValue[A], fv: FromValue[B], toSchema: ToSchema[S])
  : Either[Error, B] =
    for {
      s <- toSchema(schema)
      res <- convertWithSchemaAux(a, s)
        .toEither
        .left
        .map(ErrorList)
    } yield res

  private def convertWithSchemaAux[A, B](a: A, schema: SchemaType)
  (implicit tv: ToValue[A], fv: FromValue[B]): Validated[List[Error], B] = {
    lazy val err = invalid(List(UnexpectedType(a.toString, schema)))

    def orInvalid(opt: Option[B]) =
      opt match {
        case Some(b) => valid(b)
        case None => err
      }

    if (tv.isNull(a)) orInvalid(if (schema.nullable) Some(fv.fromNull) else None)
    else schema match {
      case ByteType(_) => orInvalid(tv.toByte(a).map(fv.fromByte))
      case ShortType(_) => orInvalid(tv.toShort(a).map(fv.fromShort))
      case IntType(_) => orInvalid(tv.toInt(a).map(fv.fromInt))
      case LongType(_) => orInvalid(tv.toLong(a).map(fv.fromLong))
      case FloatType(_) => orInvalid(tv.toFloat(a).map(fv.fromFloat))
      case DoubleType(_) => orInvalid(tv.toDouble(a).map(fv.fromDouble))
      case DecimalType(_) => orInvalid(tv.toDecimal(a).map(fv.fromDecimal))
      case StringType(_) => orInvalid(tv.toString(a).map(fv.fromString))
      case BinaryType(_) => orInvalid(tv.toBinary(a).map(fv.fromBinary))
      case BooleanType(_) => orInvalid(tv.toBoolean(a).map(fv.fromBoolean))
      case NullType => valid(fv.fromNull)
      case ArrayType(t, _) =>
        tv.toArray(a).map((arr: List[A]) =>
          arr.traverseU((a: A) => convertWithSchemaAux(a, t)) // validated[B]
             .map((arr: List[B]) => fv.fromArray(arr)))
          .getOrElse(err)
      case MapType(k, v, _) =>
        tv.toMap(a).map((m: Map[A, A]) =>
          m.toList.traverseU(kv =>
            (convertWithSchemaAux(kv._1, k) |@| convertWithSchemaAux(kv._2, v)).tupled
          ).map(a => fv.fromMap(a.toMap)))
           .getOrElse(invalid(List(UnexpectedType(a.toString, schema))))
      case UnionType(ts, _) =>
        orInvalid(ts.flatMap(t =>
          convertWithSchemaAux(a, t).toList).headOption)
      case StructType(fields, _) =>
        tv.toStruct(a).map(jsonMap =>
          fields.toList.traverseU(kv =>
            jsonMap.get(kv._1).map(v =>
              convertWithSchemaAux(v, kv._2).map(v2 => (kv._1, v2))
            ).getOrElse(
              if (kv._2.nullable) valid((kv._1, fv.fromNull))
              else invalid(List(MissingField(kv._1, a)))
            )
          ).map(a => fv.fromStruct(a.toMap))
        ).getOrElse(err)
    }
  }

  implicit val schemaTypetoSchema = new ToSchema[SchemaType] {
    def apply(schema: SchemaType): Either[Error, SchemaType] =
      Right(schema)
  }
}

trait ToSchema[A] {
  def apply(a: A): Either[Error, SchemaType]
}

trait ToValue[A] {
  def toByte(a: A): Option[Byte]
  def toShort(a: A): Option[Short]
  def toInt(a: A): Option[Int]
  def toLong(a: A): Option[Long]
  def toFloat(a: A): Option[Float]
  def toDouble(a: A): Option[Double]
  def toDecimal(a: A): Option[BigDecimal]
  def toString(a: A): Option[String]
  def toBinary(a: A): Option[Array[Byte]]
  def toBoolean(a: A): Option[Boolean]

  def toArray(a: A): Option[List[A]]
  def toMap(a: A): Option[Map[A, A]]
  def toStruct(a: A): Option[Map[String, A]]

  def isNull(a: A): Boolean
}

trait FromValue[A] {
  def fromByte(a: Byte): A
  def fromShort(a: Short) : A
  def fromInt(a: Int): A
  def fromLong(a: Long): A
  def fromFloat(a: Float): A
  def fromDouble(a: Double): A
  def fromDecimal(a: BigDecimal): A
  def fromString(a: String): A
  def fromBinary(a: Array[Byte]): A
  def fromBoolean(a: Boolean): A
  def fromNull: A

  def fromArray(a: List[A]): A
  def fromMap(a: Map[A, A]): A
  def fromStruct(a: Map[String, A]): A
}
