package com.handy.schema

import Schema.ops._

trait ConvertsFrom[A] {
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

object ConvertsFrom {
  def apply[A](implicit instance: ConvertsFrom[A]) = instance

  trait Ops[A] {
    def typeClassInstance: ConvertsFrom[A]
    def self: A
    def forSchema[S](schema: S)(implicit ev: Schema[S]) = ev.coerce(self)
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
