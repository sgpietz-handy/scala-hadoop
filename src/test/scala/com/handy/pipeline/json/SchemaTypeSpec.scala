package com.handy.schema

import io.circe.syntax._
import io.circe.parser._

import org.specs2.mutable.Specification

import com.handy.schema._
import com.handy.schema.json._
import com.handy.schema.SchemaType._

class SchemaTypeSpec extends Specification {
  "convertWithSchema" >> {
    val st: SchemaType = StructType(List(
      ("a", LongType),
      ("b", BooleanType),
      ("c", ArrayType(StringType)),
      ("d", StructType(List(
        ("e", DoubleType),
        ("f", NullType)
      )))
    ))

    val json1 = parse("""
      {
        "a": 5,
        "b": true,
        "c": ["foo", "bar", "baz"],
        "d": {
          "e": 45.222,
          "f": null
        }
      }
    """).toOption.get

    val json2 = parse("""
      {
        "a": 5,
        "c": [1, 2, "3"],
        "d": {
          "e": 45.222
        }
      }
    """).toOption.get

    "parse json to fit schema specified by given SchemaType" >> {
      convertWithSchema(json1, st) shouldEqual Right(Map(
        "a" -> 5.asJson,
        "b" -> true.asJson,
        "c" -> List("foo", "bar", "baz").asJson,
        "d" -> Map(
          "e" -> 45.222.asJson,
          "f" -> None.asJson
        ).asJson
      ).asJson)
    }
    "convert improperly typed json to fit schema based on conversion rules" >> {
      convertWithSchema(json2, st) shouldEqual Right(Map(
        "a" -> 5.asJson,
        "b" -> None.asJson,
        "c" -> List("1", "2", "3").asJson,
        "d" -> Map(
          "e" -> 45.222.asJson,
          "f" -> None.asJson
        ).asJson
      ).asJson)
    }
  }
}
