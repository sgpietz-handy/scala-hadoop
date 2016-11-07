package com.handy.schema.hive

import atto._, Atto._, compat.stdlib._

import com.handy.schema._

object SchemaParser {
  val schemaParser: Parser[SchemaType] = {
      // TODO: add date types!
      string("string")                    >| (StringType(true): SchemaType)  |
      string("char")                      >| (StringType(true): SchemaType)  |
      string("varchar")                   >| (StringType(true): SchemaType)  |
      string("binary")                    >| (BinaryType(true): SchemaType)  |
      string("tinyint")                   >| (ByteType(true): SchemaType)    |
      string("smallint")                  >| (ShortType(true): SchemaType)   |
      string("int")                       >| (IntType(true): SchemaType)     |
      string("bigint")                    >| (LongType(true): SchemaType)    |
      string("boolean")                   >| (BooleanType(true): SchemaType) |
      string("float")                     >| (FloatType(true): SchemaType)   |
      string("double")                    >| (DoubleType(true): SchemaType)  |
      string("map")       ~> mapParser    -| toMapType                       |
      string("array")     ~> arrayParser  -| toArrayType                     |
      string("uniontype") ~> unionParser  -| toUnionType                     |
      string("struct")    ~> structParser -| ( f => toStructType(f))
    }

  private def toArrayType(t: SchemaType): SchemaType = ArrayType(t, true)
  private def toUnionType(t: List[SchemaType]): SchemaType = UnionType(t, true)
  private def toStructType(t: List[(String, SchemaType)]): SchemaType = StructType(t, true)
  private def toMapType(kv: (SchemaType, SchemaType)): SchemaType =
    kv match {
      case (k: PrimitiveType, v: SchemaType) => MapType(k, v, true)
      case _ => throw new Exception("invalid map type parameters")
    }

  private def angles[A](p: Parser[A]) = bracket(char('<'), p, char('>'))
  private def untilChar(c: Char) = takeWhile(_ != c)
  private val schemaField: Parser[(String, SchemaType)] =
      pairBy(untilChar(':'), char(':'), schemaParser)

  private val arrayParser = angles(schemaParser)
  private val mapParser = angles(pairBy(schemaParser, char(','), schemaParser))
  private val unionParser = angles(sepBy(schemaParser, char(',')))
  private val structParser = angles(sepBy(schemaField, char(',')))
}


