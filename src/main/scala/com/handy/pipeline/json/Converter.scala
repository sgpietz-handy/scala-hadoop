package com.handy.pipeline.json

import io.circe.Json
import io.circe.syntax._

trait Converter {
  def toLong(json: Json): Option[Long] 
  def toDouble(json: Json): Option[Double]
  def toBoolean(json: Json): Option[Boolean]
  def toString(json: Json): Option[String]

  val allowMissing: Boolean

  final def convLong(json: Json) = toLong(json).map(_.asJson)
  final def convDouble(json: Json) = toDouble(json).map(_.asJson)
  final def convBoolean(json: Json) = toBoolean(json).map(_.asJson)
  final def convString(json: Json) = toString(json).map(_.asJson)
}

object Converter {
  val defaultConverter = new Converter {
    def toLong(json: Json) = json.as[Long].toOption
    def toDouble(json: Json) = json.as[Double].toOption
    def toBoolean(json: Json) = json.as[Boolean].toOption
    def toString(json: Json) =
      json.as[String].toOption.orElse(Some(json.toString))

    val allowMissing = true
  }
}
