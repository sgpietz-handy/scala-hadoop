package com.handy.schema

import cats.data.Validated
import cats.data.Validated._
import cats.implicits._

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
case class MapType(k: PrimitiveType, v: SchemaType) extends ComplexType
case class UnionType(ts: List[SchemaType]) extends ComplexType
case class StructType(fields: List[SchemaField]) extends ComplexType

case class SchemaField(name: String, t: SchemaType, nullable: Boolean)

object SchemaType {

  def convertWithSchema[A, B, S](a: A, schema: S)
      (implicit tv: ToValue[A], fv: FromValue[B], toSchema: ToSchema[S])
      : Either[Throwable, B] =
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

    def convertStruct(values: Map[String, A], fields: List[SchemaField])
        : Validated[List[Error], List[(String, B)]] =
      fields.traverseU { case SchemaField(name, t, nullable) =>
        val value = for {
          v <- values.get(name)
          if (!tv.isNull(v))
            } yield convertWithSchemaAux(v, t)

        val res: Validated[List[Error], (String, B)] = value match {
          case None if nullable => valid((name, fv.fromNull))
          case Some(validated) => validated.map(v => (name, v))
          case None => invalid(List(MissingField(name, values)))
        }
        res
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
      case NullType => valid(fv.fromNull)
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
           .getOrElse(invalid(List(UnexpectedType(a.toString, schema))))
      case UnionType(ts) =>
        orInvalid(ts.flatMap(t =>
          convertWithSchemaAux(a, t).toList).headOption)
      case st @ StructType(fields) =>
        tv.toStruct(a)
          .map(vs => convertStruct(vs, fields).map(l => fv.fromStruct(l.toMap)))
          .getOrElse(err)


    }

  }

  implicit val schemaTypetoSchema = new ToSchema[SchemaType] {
    def apply(schema: SchemaType): Either[Error, SchemaType] =
      Right(schema)
  }
}

trait ToSchema[A] {
  def apply(a: A): Either[Throwable, SchemaType]
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
