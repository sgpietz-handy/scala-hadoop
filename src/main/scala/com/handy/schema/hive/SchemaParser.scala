package com.handy.schema.hive

import scalaz._, Scalaz.{char => _, _}
import atto._, Atto._

import com.handy.schema._

object SchemaParser {
  val schemaParser: Parser[SchemaType] = {
      // TODO: add all types!
      string("string")                                          >| StringType  |
      string("binary")                                          >| BinaryType  |
      string("tinyint")                                         >| ByteType    |
      string("smallint")                                        >| ShortType   |
      string("int ")                                            >| IntType     |
      string("bigint")                                          >| LongType    |
      string("boolean")                                         >| BooleanType |
      string("float")                                           >| FloatType   |
      string("double")                                          >| DoubleType  |
      string("array") ~> angles(schemaParser)                   -| ArrayType   |
      string("struct") ~> angles(sepBy(schemaField, char(','))) -| StructType
    }
    
  private def angles[A](p: Parser[A]) = bracket(char('<'), p, char('>'))
  private def untilChar(c: Char) = takeWhile(_ != c)
  private val schemaField: Parser[(String, SchemaType)] =
      pairBy(untilChar(':'), char(':'), schemaParser)
}
