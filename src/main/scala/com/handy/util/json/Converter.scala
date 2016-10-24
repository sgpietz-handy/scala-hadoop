package com.handy.util.json

import io.circe.Json
import io.circe.syntax._

trait Converter {
  def toLong(json: Json): Option[Long] 
  def toDouble(json: Json): Option[Double]
  def toBoolean(json: Json): Option[Boolean]
  def toString(json: Json): Option[String]

  final def convLong(json: Json) = toLong(json).map(_.asJson)
  final def convDouble(json: Json) = toDouble(json).map(_.asJson)
  final def convBoolean(json: Json) = toBoolean(json).map(_.asJson)
  final def convString(json: Json) = toString(json).map(_.asJson)
}
