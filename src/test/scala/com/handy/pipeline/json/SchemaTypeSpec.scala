package com.handy.pipeline.json

import io.circe.syntax._
import io.circe.parser._

import cats.data.Validated.Valid

import org.specs2.mutable.Specification

import com.handy.pipeline.json.SchemaType._

class SchemaTypeSpec extends Specification {
  "fitJsonToSchema" >> {
    val st = ObjectType(List(
      ("a", LongType),
      ("b", BooleanType),
      ("c", ArrayType(StringType)),
      ("d", ObjectType(List(
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
      fitJsonToSchema(json1, st) shouldEqual Valid(Map(
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
      fitJsonToSchema(json2, st) shouldEqual Valid(Map(
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
