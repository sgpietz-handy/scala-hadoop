package com.handy.schema

import io.circe.Json

import cats.data.Validated

sealed trait Error extends Throwable { self =>
  def toInvalid = Validated.invalid(List(self))
}

case class UnexpectedType[A: ConvertsFrom](schema: SchemaType, value: A)
    extends Error {
  override def getMessage = s"""
    |failed to convert json: ${value}
    |to expected schema type: ${schema}
  """.stripMargin
}

case class MissingField[A: ConvertsFrom](name: String, values: Map[String, A])
    extends Error {
  override def getMessage = s"""
    |record $values missing field $name
  """.stripMargin
}

case class SchemaParseError(msg: String) extends Error {
  override def getMessage: String = msg
}

case class ErrorList(errs: List[Error]) extends Error {
  override def getMessage = errs.mkString("\n")
}
