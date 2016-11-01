package com.handy.schema

import io.circe.Json

sealed trait Error extends Throwable

case class UnexpectedType(value: String, schema: SchemaType) extends Error {
  override def getMessage = s"""
    |failed to convert json: ${value}
    |to expected schema type: ${schema}
  """.stripMargin
}

case class ErrorList(errs: List[Error]) extends Error {
  override def getMessage = errs.mkString("\n")
}

case class MissingField[A](name: String, record: A) extends Error {
  override def getMessage = s"""
    |record $record missing field $name
  """.stripMargin
}

case class SchemaParseError(msg: String) extends Error {
  override def getMessage: String = msg
}
