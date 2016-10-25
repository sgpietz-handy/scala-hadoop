package com.handy.schema

import io.circe.{Json, JsonObject}
import io.circe.syntax._

import cats.implicits._
import cats.data.Validated
import cats.data.Validated._

sealed trait SchemaType
sealed trait PrimitiveType extends SchemaType
sealed trait ComplexType extends SchemaType
case object ByteType extends PrimitiveType
case object ShortType extends PrimitiveType
case object IntType extends PrimitiveType
case object LongType extends PrimitiveType
case object FloatType extends PrimitiveType
case object DoubleType extends PrimitiveType
case object DecimalType extends PrimitiveType
case object StringType extends PrimitiveType
case object BinaryType extends PrimitiveType
case object BooleanType extends PrimitiveType
case object NullType extends PrimitiveType
case class ArrayType(t: SchemaType) extends ComplexType
case class MapType(k: PrimitiveType, v: ComplexType) extends ComplexType
case class UnionType(ts: List[SchemaType]) extends ComplexType
case class StructType(fields: List[(String, SchemaType)]) extends ComplexType

sealed trait SchemaValue
sealed trait PrimitiveValue extends SchemaValue
sealed trait ComplexValue extends SchemaValue
case class ByteValue(v: Byte) extends PrimitiveValue
case class ShortValue(v: Short) extends PrimitiveValue
case class IntValue(v: Int) extends PrimitiveValue
case class LongValue(v: Long) extends PrimitiveValue
case class FloatValue(v: Float) extends PrimitiveValue
case class DoubleValue(v: Double) extends PrimitiveValue
case class DecimalValue(v: BigDecimal) extends PrimitiveValue
case class StringValue(v: String) extends PrimitiveValue
case class BinaryValue(v: Array[Byte]) extends PrimitiveValue
case class BooleanValue(v: Boolean) extends PrimitiveValue
case object NullValue extends PrimitiveValue
case class ArrayValue[A <: SchemaValue](v: List[A]) extends ComplexValue
case class MapValue[A <: PrimitiveValue, B <: SchemaValue](v: Map[A, B]) extends ComplexValue
case class UnionValue(v: SchemaValue) extends ComplexValue
case class StructValue(v: Map[String, SchemaValue]) extends ComplexValue

object SchemaType {
  def convertWithSchema[A, B, S](a: A, schema: S)
  (implicit tv: ToValue[A], fv: FromValue[B], toSchema: ToSchema[S])
  : Either[Error, B] =
    for {
      s <- toSchema(schema)
      res <- convertWithSchemaAux(a, s)
        .toEither
        .left
        .map(UnexpectedTypes)
    } yield res

  private def convertWithSchemaAux[A, B](a: A, schema: SchemaType)
  (implicit tv: ToValue[A], fv: FromValue[B]): Validated[List[UnexpectedType], B] = {
    lazy val err = Invalid(List(UnexpectedType(a.toString, schema)))

    def orInvalid(opt: Option[B]) =
      opt match {
        case Some(b) => Valid(b)
        case None => err
      }
    schema match {
      case ByteType => orInvalid(tv.toByte(a).map(fv.fromByte))
      case ShortType => orInvalid(tv.toShort(a).map(fv.fromShort))
      case IntType => orInvalid(tv.toInt(a).map(fv.fromInt))
      case LongType => orInvalid(tv.toLong(a).map(fv.fromLong))
      case FloatType => orInvalid(tv.toFloat(a).map(fv.fromFloat))
      case DoubleType => orInvalid(tv.toDouble(a).map(fv.fromDouble))
      case DecimalType => orInvalid(tv.toDecimal(a).map(fv.fromDecimal))
      case StringType => orInvalid(tv.toString(a).map(fv.fromString))
      case BinaryType => orInvalid(tv.toBinary(a).map(fv.fromBinary))
      case BooleanType => orInvalid(tv.toBoolean(a).map(fv.fromBoolean))
      case NullType => Valid(fv.fromNull)
      case ArrayType(t) =>
        tv.toArray(a).map((arr: List[A]) =>
          arr.traverseU((a: A) => convertWithSchemaAux(a, t)) // validated[B]
             .map((arr: List[B]) => fv.fromArray(arr)))
          .getOrElse(err)
      case MapType(k, v) =>
        tv.toMap(a).map((m: Map[A, A]) =>
          m.toList.traverseU(kv =>
            (convertWithSchemaAux(kv._1, k) |@| convertWithSchemaAux(kv._2, v)).tupled
          ).map(a => fv.fromMap(a.toMap)))
           .getOrElse(Invalid(List(UnexpectedType(a.toString, schema))))
      case UnionType(ts) =>
        orInvalid(ts.flatMap(t =>
          convertWithSchemaAux(a, t).toList).headOption)
      case StructType(fields) =>
        tv.toStruct(a).map(st =>
          fields.toList.traverseU(kv =>
            st.get(kv._1).map(v =>
              convertWithSchemaAux(v, kv._2).map(v2 => (kv._1, v2))
            ).getOrElse(Valid((kv._1, fv.fromNull)))
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
