package com.handy.schema

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
