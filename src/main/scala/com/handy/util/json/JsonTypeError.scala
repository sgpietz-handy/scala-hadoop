package com.handy.util.json

import io.circe.Json

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

