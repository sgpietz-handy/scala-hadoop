package com.handy.schema

import cats.data.Validated
import cats.data.Validated.valid
import cats.implicits._
import simulacrum._

@typeclass trait Schema[S] {
  def apply(a: S): Either[Throwable, SchemaType]

  @op("convert") def convert[A : ConvertsFrom, B : ConvertsTo](s: S, from: A)
      : Either[Throwable, B] =
    for {
      schema <- apply(s)
      res <- convertAux(schema, from)
      .toEither
      .left
      .map(ErrorList)
    } yield res

  @op("coerce") def coerce[A : ConvertsFrom : ConvertsTo](s: S, from: A)
      : Either[Throwable, A] = convert(s, from)

  private def convertAux[A, B](schema: SchemaType, a: A)
      (implicit cf: ConvertsFrom[A], ct: ConvertsTo[B]): Validated[List[Error], B] = {

    def orInvalid(opt: Option[B]) =
      opt match {
        case Some(b) => valid(b)
        case None => UnexpectedType(schema, a).toInvalid
      }

    def convertStruct(values: Map[String, A], fields: List[SchemaField])
        : Validated[List[Error], List[(String, B)]] =
      fields.traverseU { case SchemaField(name, t, nullable) =>
        val value = for {
          v <- values.get(name)
          if (!cf.isNull(v))
            } yield convertAux(t, v)

        value match {
          case None if nullable => valid((name, ct.fromNull))
          case Some(validated) => validated.map(v => (name, v))
          case None => MissingField(name, values).toInvalid
        }
      }

    schema match {
      case ByteType => orInvalid(cf.toByte(a).map(ct.fromByte))
      case ShortType => orInvalid(cf.toShort(a).map(ct.fromShort))
      case IntType => orInvalid(cf.toInt(a).map(ct.fromInt))
      case LongType => orInvalid(cf.toLong(a).map(ct.fromLong))
      case FloatType => orInvalid(cf.toFloat(a).map(ct.fromFloat))
      case DoubleType => orInvalid(cf.toDouble(a).map(ct.fromDouble))
      case DecimalType => orInvalid(cf.toDecimal(a).map(ct.fromDecimal))
      case StringType => orInvalid(cf.toString(a).map(ct.fromString))
      case BinaryType => orInvalid(cf.toBinary(a).map(ct.fromBinary))
      case BooleanType => orInvalid(cf.toBoolean(a).map(ct.fromBoolean))
      case NullType => valid(ct.fromNull)
      case ArrayType(t) =>
        cf.toArray(a).map(arr =>
          arr.traverseU(a => convertAux(t, a)) // validated[B]
             .map(arr => ct.fromArray(arr)))
          .getOrElse(UnexpectedType(schema, a).toInvalid)
      case MapType(k, v) =>
        cf.toMap(a).map((m: Map[A, A]) =>
          m.toList.traverseU(kv =>
            (convertAux(k, kv._1) |@| convertAux(v, kv._2)).tupled
          ).map(a => ct.fromMap(a.toMap)))
           .getOrElse(UnexpectedType(schema, a).toInvalid)
      case UnionType(ts) =>
        orInvalid(ts.flatMap(t =>
          convertAux(t, a).toList).headOption)
      case st @ StructType(fields) =>
        cf.toStruct(a)
          .map(vs => convertStruct(vs, fields).map(l => ct.fromStruct(l.toMap)))
          .getOrElse(UnexpectedType(schema, a).toInvalid)
    }
  }
}
