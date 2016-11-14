package com.handy.schema

import io.circe.syntax._
import io.circe.parser._

import cats.implicits._

import org.specs2.mutable.Specification

import com.handy.schema.implicits._
import com.handy.schema.hive._
import com.handy.schema._

class SchemaTypeSpec extends Specification {
  "fitJsonToHiveSchema" >> {
      "convert json data to match a hive schema" >> {
        val hSchema = HiveSchema("""
        struct<name:string,age:int,married:boolean,numbers:array<double>,address:struct<street:string,house_no:bigint>>
        """.trim)
        val json = parse("""
          {
            "name": "Bill Murray",
            "age": 55,
            "married": true,
            "numbers": [1, 2, 3, 4, 5],
            "address": {
              "street": "elm"
            }
          }
        """).toOption.get

        hSchema.coerce(json) shouldEqual Right(Map(
          "name" -> "Bill Murray".asJson,
          "age" -> 55.asJson,
          "married" -> true.asJson,
          "numbers" -> List(1, 2, 3, 4, 5).asJson,
          "address" -> Map(
            "street" -> "elm".asJson,
            "house_no" -> None.asJson
          ).asJson
        ).asJson)
      }
    }

  "convertWithSchema" >> {
    val st: SchemaType = StructType(List(
      SchemaField("a", LongType, true),
      SchemaField("b", BooleanType, true),
      SchemaField("c", ArrayType(StringType), true),
      SchemaField("d", StructType(List(
        SchemaField("e", DoubleType, false),
        SchemaField("f", NullType, true)
      )), true)
    ))

    "parse json to fit schema specified by given SchemaType" >> {
      val json = parse("""
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

      st.coerce(json) shouldEqual Right(Map(
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
      val json = parse("""
        {
          "a": 5,
          "c": [1, 2, "3"],
          "d": {
            "e": 45.222
          }
        }
      """).toOption.get

      st.coerce(json) shouldEqual Right(Map(
        "a" -> 5.asJson,
        "b" -> None.asJson,
        "c" -> List("1", "2", "3").asJson,
        "d" -> Map(
          "e" -> 45.222.asJson,
          "f" -> None.asJson
        ).asJson
      ).asJson)
    }

    "allow null values if nullable = true" >> {
      val json = parse("""
        {
          "a": null,
          "c": [1, 2, "3"],
          "d": {
            "e": 45.222
          }
        }
      """).toOption.get

      st.coerce(json) must beRight
    }
    "disallow null values if nullable = false" >> {
      val json = parse("""
        {
          "a": 5,
          "c": [1, 2, "3"],
          "d": {
            "e": null
          }
        }
      """).toOption.get

      st.coerce(json) must beLeft
    }
    "disallow missing value if field is nullable" >> {
      val json = parse("""
        {
          "a": 5,
          "b": true,
          "c": [1, 2, "3"],
          "d": {
            "f": null
          }
        }
      """).toOption.get

      st.coerce(json) must beLeft
    }
  }

}
