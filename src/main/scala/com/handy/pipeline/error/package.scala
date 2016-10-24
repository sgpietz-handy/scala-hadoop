package com.handy.pipeline

import io.circe.Json

import com.handy.pipeline.json.SchemaType

package object error {
  trait JsonTypeError extends Throwable

  case class MissingField(name: String, json: Json)
  extends JsonTypeError {
    override def getMessage = s"""
      |json object $json missing field $name
    """.stripMargin
  }
  case class UnexpectedType(json: Json, schema: SchemaType)
  extends JsonTypeError {
    override def getMessage = s"""
      |failed to convert json: ${json.toString}
      |to expected schema type: ${schema}
    """.stripMargin
  }

  case class SchemaParseError(msg: String) extends Throwable {
    override def getMessage: String = msg
  }
}
