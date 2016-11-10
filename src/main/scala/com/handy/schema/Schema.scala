package com.handy.schema

import cats.data.Validated
import cats.data.Validated._
import cats.implicits._


trait Schema[S] {
  def apply(a: S): Either[Throwable, SchemaType]

  def convert[A, B : ConvertsTo](s: S, from: A)
             (implicit from: ConvertsFrom[A]): Either[Throwable, B] =
    for {
      schema <- apply(s)
      res <- convertWithSchemaAux(from, schema)
      .toEither
      .left
      .map(ErrorList)
    } yield res

  def coerce[A : ConvertsFrom : ConvertsTo](s: S, from: A): Either[Throwable, A] = convert(from)

  private def convertWithSchemaAux[A, B](a: A, schema: SchemaType)
  (implicit cf: ConvertsFrom[A], ct: ConvertsTo[B]): Validated[List[Error], B] = {
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
          if (!cf.isNull(v))
            } yield convertWithSchemaAux(v, t)

        val res: Validated[List[Error], (String, B)] = value match {
          case None if nullable => valid((name, ct.fromNull))
          case Some(validated) => validated.map(v => (name, v))
          case None => invalid(List(MissingField(name, values)))
        }
        res
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
        cf.toArray(a).map((arr: List[A]) =>
          arr.traverseU((a: A) => convertWithSchemaAux(a, t)) // validated[B]
             .map((arr: List[B]) => ct.fromArray(arr)))
          .getOrElse(err)
      case MapType(k, v) =>
        cf.toMap(a).map((m: Map[A, A]) =>
          m.toList.traverseU(kv =>
            (convertWithSchemaAux(kv._1, k) |@| convertWithSchemaAux(kv._2, v)).tupled
          ).map(a => ct.fromMap(a.toMap)))
           .getOrElse(invalid(List(UnexpectedType(a.toString, schema))))
      case UnionType(ts) =>
        orInvalid(ts.flatMap(t =>
          convertWithSchemaAux(a, t).toList).headOption)
      case st @ StructType(fields) =>
        cf.toStruct(a)
          .map(vs => convertStruct(vs, fields).map(l => ct.fromStruct(l.toMap)))
          .getOrElse(err)
    }
  }
}

object Schema {
  def apply[S](implicit instance: Schema[S]) = instance

  trait Ops[S] {
    def typeClassInstance: Schema[S]
    def self: A
    def convert[A: ConvertsFrom, B: ConvertsTo](from: A): B =
      typeClassInstance.convert(self, from)
    def corece[A : ConvertsFrom : ConvertsTo](from: A): B =
      typeClassInstance.corece(self, from)
  }

  object ops {
    implicit def toSchemaOps[S](target: S)(implicit tc: Schema[S]): Ops[S] =
      new Ops[S] {
        val self = target
        val typeClassInstance = tc
      }
  }

  implicit val schemaTypetoSchema = new Schema[SchemaType] {
    def apply(schema: SchemaType): Either[Error, SchemaType] =
      Right(schema)
  }
}
